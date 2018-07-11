/*
 * Copyright 2014–2018 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.regression

import slamdata.Predef._
import quasar._
import quasar.api._
import quasar.build.BuildInfo
import quasar.common.PhaseResults
import quasar.contrib.argonaut._
import quasar.contrib.fs2.convert
import quasar.contrib.fs2.stream._
import quasar.contrib.iota._
import quasar.contrib.nio.{file => contribFile}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.{MonadError_, MonadTell_}
import quasar.contrib.scalaz.concurrent.task._
import quasar.ejson
import quasar.ejson.Common.{Optics => CO}
import quasar.fp._
import quasar.fs.FileSystemType
import quasar.impl.datasource.local.LocalType
import quasar.impl.external.ExternalConfig
import quasar.run.{Quasar, QuasarError, SqlQuery}
import quasar.sql.Query

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.matching.Regex

import java.math.{MathContext, RoundingMode}
import java.nio.file.{Files, Path => JPath, Paths}

import scala.Predef.=:=

import argonaut._, Argonaut._
import cats.effect.{ConcurrentEffect, Effect, IO, Sync, Timer}
import cats.effect.concurrent.Deferred
import eu.timepit.refined.auto._
import fs2.{io, text, Stream}
import matryoshka._
import org.specs2.execute
import org.specs2.specification.core.Fragment
import pathy.Path, Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
// import shims._ causes compilation to not terminate in any reasonable amount of time.
import shims.{monadErrorToScalaz, monadToScalaz}

final class Sql2QueryRegressionSpec extends Qspec {
  import Sql2QueryRegressionSpec._

  implicit val ignorePhaseResults: MonadTell_[IO, PhaseResults] =
    MonadTell_.ignore[IO, PhaseResults]

  implicit val ioQuasarError: MonadError_[IO, QuasarError] =
    MonadError_.facet[IO](QuasarError.throwableP)

  // Make the `join` syntax from Monad ambiguous.
  implicit class NoJoin[F[_], A](ffa: F[A]) {
    def join(implicit ev: A =:= F[A]): F[A] = ???
  }

  val DataDir = rootDir[Sandboxed] </> dir("local")

  /** A name to identify the suite in test output. */
  val suiteName: String = "SQL^2 Regression Queries"

  val Q = for {
    tmpPath <-
      Stream.bracket(IO(Files.createTempDirectory("quasar-test-")))(
        contribFile.deleteRecursively[IO](_))

    q <- Quasar[IO](tmpPath, ExternalConfig.Empty, global)

    localCfg =
      ("rootDir" := posixCodec.printPath(TestDataRoot)) ->:
      // 64 KB chunks, chosen mostly arbitrarily.
      ("readChunkSizeBytes" := 65535) ->:
      jEmptyObject

    _ <- Stream.eval(q.dataSources.add(
      ResourceName("local"),
      LocalType,
      localCfg,
      ConflictResolution.Preserve))
  } yield q

  val lwcLocalConfigs =
    Effect[Task].toIO(TestConfig.fileSystemConfigs(FileSystemType("lwc_local")))

  ////

  val buildSuite =
    lwcLocalConfigs.map(_.isEmpty).ifM(
      IO(suiteName >> skipped("to run, enable the 'lwc_local' test configuration.")),
      for {
        qdef <- Deferred[IO, Quasar[IO, IO]]
        sdown <- Deferred[IO, Unit]

        tests <- regressionTests[IO](TestsRoot, TestDataRoot)

        _ <- Q.evalMap(q => qdef.complete(q) *> sdown.get).compile.drain.start
        q <- qdef.get

        f = (squery: SqlQuery) =>
          Stream.eval(q.queryEvaluator.evaluate(squery))
            .flatMap(_.valueOr(err =>
              Stream.raiseError(new RuntimeException(err.shows)).covary[IO]))

      } yield {
        suiteName >> {
          tests.toList foreach { case (loc, test) =>
            regressionExample(loc, test, BackendName("mimir"), f)
          }

          step(sdown.complete(()).unsafeRunSync())
        }
      })

  buildSuite.unsafeRunSync()

  ////

  /** Returns an `Example` verifying the given `RegressionTest`. */
  def regressionExample(
      loc: RFile,
      test: RegressionTest,
      backendName: BackendName,
      results: SqlQuery => Stream[IO, Data])
      : Fragment = {
    def runTest: execute.Result = {
      val query =
        SqlQuery(
          Query(test.query),
          Variables.fromMap(test.variables),
          test.data.nonEmpty.fold(DataDir </> fileParent(loc), rootDir))

      verifyResults(test.expected, results(query), backendName)
        .timed(2.minutes)
        .unsafePerformSync
    }

    s"${test.name} [${posixCodec.printPath(loc)}]" >> {
      collectFirstDirective(test.backends, backendName) {
        case TestDirective.Skip    => skipped
        case TestDirective.SkipCI  =>
          BuildInfo.isCIBuild.fold(execute.Skipped("(skipped during CI build)"), runTest)
        case TestDirective.Timeout =>
          // NB: To locally skip tests that time out, make `Skipped` unconditional.
          BuildInfo.isCIBuild.fold(
            execute.Skipped("(skipped because it times out)"),
            runTest)
        case TestDirective.Pending | TestDirective.PendingIgnoreFieldOrder =>
          runTest.pendingUntilFixed
      } getOrElse runTest
    }
  }

  /** Verify the given results according to the provided expectation. */
  def verifyResults(
      exp: ExpectedResult,
      act: Stream[IO, Data],
      backendName: BackendName)
      : Task[execute.Result] = {

    /** This helps us get identical results on different connectors, even though
      * they have different precisions for their floating point values.
      */
    val reducePrecision =
      λ[EndoK[ejson.Common]](CO.dec.modify(_.round(TestContext))(_))

    val normalizeJson: Json => Json =
      j => Recursive[Json, ejson.Json].transCata(j)(liftFFCopK(reducePrecision[Json]))

    val deleteFields: Json => Json =
      _.withObject(exp.ignoredFields.foldLeft(_)(_ - _))

    val fieldOrderSignificance =
      if (exp.ignoreFieldOrder)
        OrderIgnored
      else
        collectFirstDirective[OrderSignificance](exp.backends, backendName) {
          case TestDirective.IgnoreAllOrder |
               TestDirective.IgnoreFieldOrder |
               TestDirective.PendingIgnoreFieldOrder =>
            OrderIgnored
        } | OrderPreserved

    val resultOrderSignificance =
      if (exp.ignoreResultOrder)
        OrderIgnored
      else
        collectFirstDirective[OrderSignificance](exp.backends, backendName) {
          case TestDirective.IgnoreAllOrder |
               TestDirective.IgnoreResultOrder |
               TestDirective.PendingIgnoreFieldOrder =>
            OrderIgnored
        } | OrderPreserved

    val actProcess =
      convert.toProcess(act)
        // TODO{fs2}: Chunkiness
        .map(normalizeJson <<< deleteFields <<< (_.asJson))
        .translate(λ[IO ~> Task](_.to[Task]))

    val result =
      exp.predicate(
        exp.rows.toVector,
        actProcess,
        fieldOrderSignificance,
        resultOrderSignificance)

    collectFirstDirective(exp.backends, backendName) {
      case TestDirective.Timeout =>
        result.map {
          case execute.Success(_, _) =>
            execute.Failure(s"Fixed now, you should remove the “timeout” status.")

          case execute.Failure(m, _, _, _) =>
            execute.Failure(s"Failed with “$m”, you should change the “timeout” status.")

          case x => x
        }.handle {
          case e: java.util.concurrent.TimeoutException =>
            execute.Pending(s"times out: ${e.getMessage}")

          case e =>
            execute.Failure(s"Errored with “${e.getMessage}”, you should change the “timeout” status to “pending”.")
        }
    } getOrElse result.handle {
      case e: java.util.concurrent.TimeoutException =>
        execute.Failure(s"Times out (${e.getMessage}), you should use the “timeout” status.")
    }
  }

  /** Returns all the `RegressionTest`s found in the given directory, keyed by
    * file path.
    */
  def regressionTests[F[_]: ConcurrentEffect: Timer](
      testDir: RDir,
      dataDir: RDir)
      : F[Map[RFile, RegressionTest]] =
    descendantsMatching[F](testDir, TestPattern)
      .map(f => loadRegressionTest[F](f).map((f.relativeTo(dataDir).get, _)))
      .join(12)
      .compile
      .fold(Map[RFile, RegressionTest]())(_ + _)

  /** Loads a `RegressionTest` from the given file. */
  def loadRegressionTest[F[_]: Effect: Timer](file: RFile): Stream[F, RegressionTest] =
    io.file.readAllAsync[F](jPath(file), 8192)
      .through(text.utf8Decode)
      .reduce(_ + _)
      .flatMap(txt =>
        decodeJson[RegressionTest](txt).fold(
          err => Stream.raiseError(new RuntimeException(posixCodec.printPath(file) + ": " + err)),
          Stream.emit).covary[F])

  /** Returns all descendant files in the given dir matching `pattern`. */
  def descendantsMatching[F[_]: Sync](d: RDir, pattern: Regex): Stream[F, RFile] =
    convert.fromJavaStream(Sync[F].delay(Files.walk(jPath(d))))
      .filter(p => pattern.findFirstIn(p.getFileName.toString).isDefined)
      .map(p => posixCodec.parseRelFile(p.toString).flatMap(_.relativeTo(d)))
      .unNone
      .map(d </> _)

  /** Whether the backend has the specified directive in the given test. */
  def hasDirective(td: TestDirective, dirs: Directives, name: BackendName): Boolean =
    Foldable[Option].compose[NonEmptyList].any(dirs get name)(_ ≟ td)

  /** Returns the result of the given partial function applied to the first directive
    * for which it is defined, or `None` if it is undefined for all directives.
    */
  def collectFirstDirective[A](
      dirs: Directives,
      name: BackendName)(
      pf: PartialFunction[TestDirective, A])
      : Option[A] =
    Foldable[Option].compose[NonEmptyList]
      .findMapM[Id, TestDirective, A](dirs get name)(pf.lift)(idInstance)

  def jPath(p: Path[_, _, Sandboxed]): JPath =
    Paths.get(posixCodec.printPath(p))
}

object Sql2QueryRegressionSpec {
  val TestDataRoot: RDir =
    currentDir[Sandboxed] </> dir("it") </> dir("src") </> dir("main") </> dir("resources") </> dir("tests")

  val TestsRoot: RDir =
    TestDataRoot

  val TestPattern: Regex =
    """^([^.].*)\.test""".r

  val TestContext: MathContext =
    new MathContext(13, RoundingMode.DOWN)

  implicit val dataEncodeJson: EncodeJson[Data] =
    EncodeJson(DataCodec.Precise.encode(_).getOrElse(jString("Undefined")))
}
