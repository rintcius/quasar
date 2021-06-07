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

package quasar.impl.discovery

import quasar.ScalarStages
import quasar.api.discovery._
import quasar.api.resource._
import quasar.connector.ResourceSchema
import quasar.connector.datasource.Loader
import quasar.impl.QuasarDatasource
import quasar.qscript.InterpretedRead

import scala.{Boolean, Option}
import scala.util.Either

import cats.Monad
import cats.data.{EitherT, OptionT}
import cats.effect.Resource

final class DefaultDiscovery[
    F[_]: Monad, G[_],
    I, S <: SchemaConfig, R] private (
    quasarDatasource: I => F[Option[QuasarDatasource[Resource[F, ?], G, R, ResourcePathType]]],
    schema: ResourceSchema[F, S, (ResourcePath, R)])
    extends Discovery[Resource[F, ?], G, I, S] {

  import DiscoveryError._

  type PathType = ResourcePathType

  def pathIsResource(i: I, path: ResourcePath)
      : Resource[F, Either[DatasourceNotFound[I], Boolean]] =
    lookupDatasource[DatasourceNotFound[I]](i)
      .semiflatMap(_.pathIsResource(path))
      .value

  def prefixedChildPaths(i: I, prefixPath: ResourcePath)
      : Resource[F, Either[DiscoveryError[I], G[(ResourceName, ResourcePathType)]]] =
    lookupDatasource[DiscoveryError[I]](i)
      .flatMap(qds =>
        OptionT(qds.prefixedChildPaths(prefixPath))
          .toRight(pathNotFound[I](prefixPath)))
      .value

  def resourceSchema(i: I, path: ResourcePath, schemaConfig: S)
      : Resource[F, Either[DiscoveryError[I], schemaConfig.Schema]] = {
    val discoverSchema = for {
      ds <- lookupDatasource(i)

      result <- EitherT.right[DiscoveryError[I]]{
        ds.loaders.head match {
          case Loader.Batch(b) => b.loadFull(InterpretedRead(path, ScalarStages.Id))
        }
      }

      s <- EitherT.right[DiscoveryError[I]](Resource.liftF(schema(schemaConfig, (path, result))))
    } yield s

    discoverSchema.value
  }

  ////

  private type DS = QuasarDatasource[Resource[F, ?], G, R, ResourcePathType]

  private def lookupDatasource[E >: DatasourceNotFound[I] <: DiscoveryError[I]](i: I)
      : EitherT[Resource[F, ?], E, DS] =
    OptionT(Resource.liftF(quasarDatasource(i))).toRight(datasourceNotFound[I, E](i))
}

object DefaultDiscovery {
  def apply[
      F[_]: Monad, G[_],
      I, S <: SchemaConfig, R](
      quasarDatasource: I => F[Option[QuasarDatasource[Resource[F, ?], G, R, ResourcePathType]]],
      schema: ResourceSchema[F, S, (ResourcePath, R)])
      : Discovery[Resource[F, ?], G, I, S] =
    new DefaultDiscovery(quasarDatasource, schema)
}
