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

package quasar
package repl2

import slamdata.Predef._
import quasar.impl.external.ExternalConfig
import quasar.common.PhaseResults
import quasar.contrib.scalaz.{MonadError_, MonadTell_}
import quasar.run.{Quasar, QuasarApp, QuasarError}

import java.nio.file.Paths
import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.{ExitCode, IO}
import cats.effect.concurrent.Ref
import fs2.Stream
import scalaz._, Scalaz._
import shims._

object Main extends QuasarApp {

  implicit val ignorePhaseResults: MonadTell_[IO, PhaseResults] =
    MonadTell_.ignore[IO, PhaseResults]

  implicit val ioQuasarError: MonadError_[IO, QuasarError] =
    MonadError_.facet[IO](QuasarError.throwableP)

  def quasarStream: Stream[IO, Quasar[IO, IO]] =
    for {
      //TODO parse from command line
      tmpPath <- Stream.eval(IO(Paths.get("/tmp/quasar-repl")))
      q <- Quasar[IO](tmpPath, ExternalConfig.Empty, global)
    } yield q

  def repl(q: Quasar[IO, IO]): IO[ExitCode] =
    for {
      ref <- Ref.of[IO, ReplState](ReplState.mk)
      repl <- Repl.mk[IO](q.dataSources, ref)
      l <- repl.loop
    } yield l

  def run(args: List[String]): IO[ExitCode] =
    runQuasar(quasarStream, repl)

}
