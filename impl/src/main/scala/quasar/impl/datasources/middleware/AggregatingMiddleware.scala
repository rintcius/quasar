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

package quasar.impl
package datasources.middleware

import quasar.api.resource.ResourcePathType
import quasar.connector.MonadResourceErr
import quasar.impl.QuasarDatasource
import quasar.impl.datasource.{AggregateResult, AggregatingDatasource, MonadCreateErr}
import quasar.qscript.InterpretedRead

import scala.util.Either

import cats.effect.{Resource, Sync}

import fs2.Stream

object AggregatingMiddleware {
  def apply[T[_[_]], F[_]: MonadResourceErr: MonadCreateErr: Sync, I, R](
      datasourceId: I,
      ds: QuasarDatasource[Resource[F, ?], Stream[F, ?], R, ResourcePathType.Physical])
      : F[QuasarDatasource[Resource[F, ?], Stream[F, ?], Either[R, AggregateResult[F, R]], ResourcePathType]] =
    Sync[F].pure {
        AggregatingDatasource(ds, InterpretedRead.path)
    }
}
