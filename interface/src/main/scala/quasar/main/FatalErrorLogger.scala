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

package quasar.main

import slamdata.Predef._
import quasar.effect._
import quasar.fp.free._
import quasar.fs._

import org.slf4s.Logger
import scalaz.{Failure => _, _}

@simulacrum.typeclass trait FatalErrorLogger[IN[_]] {
  def log[F[_]: Applicative: Capture, S[_]](
    implicit
    F: F :<: S,
    IN: IN :<: S)
      : IN ~> Free[S, ?]
}

object FatalErrorLogger extends FatalErrorLoggerInstances {
  def log[IN[_], F[_]: Applicative: Capture, S[_]](
    implicit
    F: F :<: S,
    IN: IN :<: S)
      : IN ~> Free[S, ?] =
    FatalErrorLogger[IN].log[F, S]

  def log0[F[_]: Applicative: Capture, S[_]](
    implicit
    F: F :<: S)
      : S ~> Free[S, ?] =
    log[S, F, S]

}

sealed abstract class FatalErrorLoggerInstances extends FatalErrorLoggerInstancesʹ {

  implicit def physicalError(logger: Logger): FatalErrorLogger[Failure[PhysicalError, ?]] =
    new FatalErrorLogger[Failure[PhysicalError, ?]] {
      override def log[F[_]: Applicative: Capture, S[_]](
        implicit
        F: F :<: S,
        IN: Failure[PhysicalError, ?] :<: S)
          : Failure[PhysicalError, ?] ~> Free[S, ?] =
        logging.logPhysicalError[F, S](logger)
    }

  implicit def coproduct[G[_], H[_]](
    implicit
    G: FatalErrorLogger[G],
    H: FatalErrorLogger[H])
      : FatalErrorLogger[Coproduct[G, H, ?]] =
    new FatalErrorLogger[Coproduct[G, H, ?]] {
      override def log[F[_]: Applicative: Capture, S[_]](
        implicit
        F: F :<: S,
        IN: Coproduct[G, H, ?] :<: S)
          : Coproduct[G, H, ?] ~> Free[S, ?] = {

        @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
        implicit val GS: G :<: S = slamdata.Predef.???
        @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
        implicit val HS: H :<: S = slamdata.Predef.???

        λ[Coproduct[G, H, ?] ~> Free[S, ?]](_.run.fold(
          G.log[F, S],
          H.log[F, S]
        ))
      }
    }
}

sealed abstract class FatalErrorLoggerInstancesʹ {
  implicit def default[IN[_]]: FatalErrorLogger[IN] =
    new FatalErrorLogger[IN] {
      override def log[F[_]: Applicative: Capture, S[_]](
        implicit
        F: F :<: S,
        IN: IN :<: S)
          : IN ~> Free[S, ?] =
        injectFT[IN, S]
    }
}
