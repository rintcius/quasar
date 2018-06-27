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

package quasar.repl2

import slamdata.Predef._
import quasar.fp.ski._

import cats.effect._
import cats.effect.concurrent.Ref
import eu.timepit.refined.refineV
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import scalaz._, Scalaz._

final class Evaluator[F[_]: Monad: Effect](
  stateRef: Ref[F, ReplState]) {

  import Command._
  import Evaluator._

  val F = Effect[F]

  def evaluate(cmd: Command): F[Result] = {
    val eval: F[Option[String]] = cmd match {
      case Help =>
        F.pure(helpMsg.some)

      case Debug(level) =>
        stateRef.update(_.copy(debugLevel = level)) *>
          F.pure(s"Set debug level: $level".some)

      case SummaryCount(rows) =>
        val count: Option[Option[Int Refined Positive]] =
          if (rows === 0) Some(None)
          else refineV[Positive](rows).fold(κ(None), p => Some(Some(p)))
        count match {
          case None => F.pure("Rows must be a positive integer or 0 to indicate no limit".some)
          case Some(c) => stateRef.update(_.copy(summaryCount = c)) *>
            F.pure(s"Set rows to show in result: $rows".some)
        }

      case Exit =>
        F.pure("Exiting...".some)

      case _ =>
        current(stateRef) *>
        F.pure(s"TODO: $cmd".some)
    }
    val exitCode = if (cmd === Exit) Some(ExitCode.Success) else None

    eval.map(Result(exitCode, _))
  }

  ////

  private def current(ref: Ref[F, ReplState]) =
    for {
      s <- ref.get
      _ <- F.delay(println(s"Current: $s"))
    } yield ()
}

object Evaluator {
  final case class Result(exitCode: Option[ExitCode], string: Option[String])

  def apply[F[_]: Monad: Effect](
    stateRef: Ref[F, ReplState]):
      Evaluator[F] =
    new Evaluator[F](stateRef)

  val helpMsg =
    """Quasar REPL, Copyright © 2014–2018 SlamData Inc.
      |
      |Available commands:
      |  exit
      |  help
      |  cd [path]
      |  [query]
      |  [id] <- [query]
      |  explain [query]
      |  compile [query]
      |  ls [path]
      |  set debug = 0 | 1 | 2
      |  set phaseFormat = tree | code
      |  set timingFormat = tree | onlytotal
      |  set summaryCount = [rows]
      |  set format = table | precise | readable | csv
      |  set [var] = [value]
      |  env""".stripMargin
}
