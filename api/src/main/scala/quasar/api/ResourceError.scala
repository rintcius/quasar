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

package quasar.api

import slamdata.Predef.{None, Product, Serializable, Some}

import scalaz.{Equal, Cord, Show}
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.show._

sealed trait ResourceError extends Product with Serializable

object ResourceError extends ResourceErrorInstances {
  sealed trait ReadError extends ResourceError

  final case class NotAResource(path: ResourcePath) extends ReadError

  sealed trait CommonError extends ReadError

  final case class PathNotFound(path: ResourcePath) extends CommonError

  def notAResource[E >: ReadError](path: ResourcePath): E =
    NotAResource(path)

  def pathNotFound[E >: CommonError](path: ResourcePath): E =
    PathNotFound(path)
}

sealed abstract class ResourceErrorInstances {
  import ResourceError._

  implicit def equal[E <: ResourceError]: Equal[E] =
    Equal.equalBy {
      case NotAResource(p) => (Some(p), None)
      case PathNotFound(p) => (None, Some(p))
    }

  implicit def show[E <: ResourceError]: Show[E] =
    Show.show {
      case NotAResource(p) =>
        Cord("NotAResource(") ++ p.show ++ Cord(")")

      case PathNotFound(p) =>
        Cord("PathNotFound(") ++ p.show ++ Cord(")")
    }
}
