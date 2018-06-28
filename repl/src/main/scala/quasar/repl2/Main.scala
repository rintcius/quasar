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
import quasar.api._

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.concurrent.Ref
import scalaz._, Scalaz._
import shims._

import cats.syntax.applicative._
import cats.data.StateT
import quasar.contrib.scalaz.MonadState_

object Main extends IOApp {
  // TODO clean up once we can work with real datasources instead of MockDataSources
  type Store = IMap[ResourceName, (DataSourceMetadata, String)]
  type DataSourcesMonadT[A] = StateT[IO, Store, A]

  implicit val storeValueSemiGroup: Semigroup[(DataSourceMetadata, String)] =
    new Semigroup[(DataSourceMetadata, String)] {
      def append(
        f1: (DataSourceMetadata, String),
        f2: => (DataSourceMetadata,String))
          : (DataSourceMetadata, String) = f1
    }

  implicit def catsMonadState_[F[_]: cats.Applicative, S]: MonadState_[StateT[F, S, ?], S] = new MonadState_[StateT[F, S, ?], S] {
    def get : StateT[F, S, S] = StateT[F, S, S](s => (s,s).pure[F] )
    def put(s: S): StateT[F, S, Unit] = StateT.set[F, S](s)
  }

  def mock: (DataSources[DataSourcesMonadT, String], IMap[ResourceName, (DataSourceMetadata, String)]) = {
    import DataSourceError._
    import eu.timepit.refined.auto._

    val s3 = DataSourceType("s3", 3L)
    val azure = DataSourceType("azure", 3L)
    val mongo = DataSourceType("mongodb", 3L)
    val acceptsSet = ISet.fromList(List(s3, azure, mongo))

     def errorCondition(
         rn: ResourceName,
         dst: DataSourceType,
         config: String
         ): Condition[InitializationError[String]] = {
       (rn.value, config) match {
         case ("bar", "bad-s3-config") =>
           Condition.abnormal[InitializationError[String]](MalformedConfiguration(dst, config, "Malformed DataSource configuration"))
         case (_, _) =>
           Condition.normal()
       }
     }

     val dataSources = MockDataSources[DataSourcesMonadT, String](acceptsSet, errorCondition)

     val initialStore =
       IMap[ResourceName, (DataSourceMetadata, String)]((ResourceName("tests3"), (DataSourceMetadata(s3, Condition.Normal()), "test-s3-config")))

     (dataSources, initialStore)
  }

  def run(args: List[String]): IO[ExitCode] = {
    val datasources = mock._1
    val l = Ref.of[DataSourcesMonadT, ReplState](ReplState.mk) >>=
      (ref => Repl.mk[DataSourcesMonadT, String](datasources, ref).loop)
    l.runA(mock._2)
  }
}
