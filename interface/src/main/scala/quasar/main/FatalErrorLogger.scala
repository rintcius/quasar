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

import quasar.effect._
import quasar.fp.free._
import quasar.fs._

import org.slf4s.Logger
import scalaz.{Failure => _, _}

@simulacrum.typeclass trait FatalErrorLogger[IN[_]] {
  type S[_]

  def log[F[_]: Applicative: Capture](
    logger: Logger)(
    implicit
    F: F :<: S)
      : IN ~> Free[S, ?]
}

object FatalErrorLogger extends FatalErrorLoggerInstances {

  type Aux[IN[_], Sʹ[_]] = FatalErrorLogger[IN] {
    type S[A] = Sʹ[A]
  }

  def log[IN[_], F[_]: Applicative: Capture, Sʹ[_]](
    logger: Logger)(
    implicit
    IN: FatalErrorLogger.Aux[IN, Sʹ],
    F: F :<: Sʹ)
      : IN ~> Free[Sʹ, ?] =
    IN.log[F](logger)

  def log0[F[_]: Applicative: Capture, S[_]](
    logger: Logger)(
    implicit
    F: F :<: S)
      : S ~> Free[S, ?] =
    log[S, F, S](logger)

}

sealed abstract class FatalErrorLoggerInstances extends FatalErrorLoggerInstancesʹ {

  implicit def physicalError[Sʹ[_]](
    implicit
    IN: Failure[PhysicalError, ?] :<: Sʹ)
      : FatalErrorLogger.Aux[Failure[PhysicalError, ?], Sʹ] =
    new FatalErrorLogger[Failure[PhysicalError, ?]] {
      type S[A] = Sʹ[A]

      override def log[F[_]: Applicative: Capture](
        logger: Logger)(
        implicit
        F: F :<: S)
          : Failure[PhysicalError, ?] ~> Free[S, ?] =
        logging.logPhysicalError[F, S](logger)
    }

  implicit def coproduct[G[_], H[_], Sʹ[_]](
    implicit
    G: FatalErrorLogger.Aux[G, Sʹ],
    H: FatalErrorLogger.Aux[H, Sʹ])
      : FatalErrorLogger[Coproduct[G, H, ?]] =
    new FatalErrorLogger[Coproduct[G, H, ?]] {
      type S[A] = Sʹ[A]

      override def log[F[_]: Applicative: Capture](
        logger: Logger)(
        implicit
        F: F :<: S)
          : Coproduct[G, H, ?] ~> Free[S, ?] = {

        λ[Coproduct[G, H, ?] ~> Free[S, ?]](_.run.fold(
          G.log[F](logger),
          H.log[F](logger)
        ))
      }
    }
}

sealed abstract class FatalErrorLoggerInstancesʹ {
  implicit def default[IN[_], Sʹ[_]](
    implicit
    IN: IN :<: Sʹ)
      : FatalErrorLogger.Aux[IN, Sʹ] =
    new FatalErrorLogger[IN] {
      type S[A] = Sʹ[A]

      override def log[F[_]: Applicative: Capture](
        logger: Logger)(
        implicit
        F: F :<: S)
          : IN ~> Free[S, ?] =
        injectFT[IN, S]
    }
}
