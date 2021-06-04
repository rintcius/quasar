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

import quasar.api.resource.{ResourcePath, ResourcePathType}
import quasar.connector.MonadResourceErr
import quasar.connector.datasource.Datasource
import quasar.impl.datasource.{AggregateResult, AggregatingDatasource, MonadCreateErr}
import quasar.qscript.{InterpretedRead, QScriptEducated}

import scala.util.{Either, Left}

import cats.effect.{Resource, Sync}

import fs2.Stream

import shims.{functorToCats, functorToScalaz}

object AggregatingMiddleware {
  def apply[T[_[_]], F[_]: MonadResourceErr: MonadCreateErr: Sync, I, R](
      datasourceId: I,
      ds: Datasource[Resource[F, ?], Stream[F, ?], InterpretedRead[ResourcePath], R, ResourcePathType.Physical])
      : F[Datasource[Resource[F, ?], Stream[F, ?], InterpretedRead[ResourcePath], Either[R, AggregateResult[F, R]], ResourcePathType]] =
    Sync[F].pure {
        AggregatingDatasource(ds, InterpretedRead.path)
    }
}
