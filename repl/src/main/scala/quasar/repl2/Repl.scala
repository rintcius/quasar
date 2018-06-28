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
import quasar.api.DataSources
import quasar.build.BuildInfo
import quasar.fp.ski._

import cats.effect._
import cats.effect.concurrent._
import org.jline.reader._
import org.jline.terminal._
import scalaz._, Scalaz._

final class Repl[F[_]: Monad: Effect](
  prompt: String,
  reader: LineReader,
  evaluator: Command => F[Evaluator.Result]) {

  val F = Effect[F]

  private val read: F[Command] = F.delay(Command.parse(reader.readLine(prompt)))

  private def eval(cmd: Command): F[Evaluator.Result] =
    F.liftIO {
      F.runSyncStep(evaluator(cmd))
        .map(_.fold(κ(Evaluator.Result(ExitCode.Error.some, "unexpected error".some)), ι))
    }

  private def print(string: Option[String]): F[Unit] =
    string.fold(F.unit)(s => F.delay(println(s)))

  val loop: F[ExitCode] =
    for {
      cmd <- read
      res <- eval(cmd)
      Evaluator.Result(exitCode, string) = res
      _ <- print(string)
      next <- exitCode.fold(loop)(_.point[F])
    } yield next

}

object Repl {
  def apply[F[_]: Monad: Effect](
    prompt: String,
    reader: LineReader,
    evaluator: Command => F[Evaluator.Result]):
      Repl[F] =
    new Repl[F](prompt, reader, evaluator)

  def mk[F[_]: Monad: Effect, C](datasources: DataSources[F, C], ref: Ref[F, ReplState]): Repl[F] = {
    val evaluator = Evaluator[F, C](ref, datasources)
    Repl[F](prompt, mkLineReader, evaluator.evaluate)
  }

  ////

  private val prompt = s"(v${BuildInfo.version}) 💪 $$ "

  private def mkLineReader: LineReader = {
    val terminal = TerminalBuilder.terminal()
    LineReaderBuilder.builder().terminal(terminal).build()
  }

}
