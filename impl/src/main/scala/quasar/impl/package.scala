/*
 * Copyright 2020 Precog Data
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

import quasar.api.resource._
import quasar.connector.datasource.Datasource
import quasar.contrib.scalaz.MonadError_
import quasar.qscript.InterpretedRead

import java.lang.String
import java.util.UUID

import monocle.Prism

import scala.util.Try

package object impl {
  val UuidString: Prism[String, UUID] =
    Prism[String, UUID](
      s => Try(UUID.fromString(s)).toOption)(
      u => u.toString)

  type MonadQuasarErr[F[_]] = MonadError_[F, QuasarError]
  def MonadQuasarErr[F[_]](implicit ev: MonadQuasarErr[F]): MonadQuasarErr[F] = ev


  private [impl] type QuasarDatasource[F[_], G[_], R, P <: ResourcePathType] =
    Datasource[F, G, InterpretedRead[ResourcePath], R, P]

}
