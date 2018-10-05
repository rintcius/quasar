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

package quasar.regression

import slamdata.Predef._
import quasar.BackendName
import quasar.fp._

import argonaut._, Argonaut._
import argonaut.DecodeJsonScalaz._
import pathy.Path, Path._
import pathy.argonaut.PosixCodecJson._
import scalaz._, Scalaz._

case class RegressionTest(
  name:      String,
  backends:  Directives,
  data:      List[RelFile[Unsandboxed]],
  query:     String,
  variables: Map[String, String],
  expected:  ExpectedResult
)

object RegressionTest {
  import DecodeResult._

  final case class OneOrMore[A](value: NonEmptyList[A])

  object OneOrMore {
    implicit def decodeJson[A: DecodeJson]: DecodeJson[OneOrMore[A]] =
      DecodeJson(c => c.as[A].map(NonEmptyList(_)) ||| c.as[NonEmptyList[A]])
        .map(OneOrMore(_))
  }

  implicit val RegressionTestDecodeJson: DecodeJson[RegressionTest] =
    DecodeJson(c => for {
      name              <- (c --\ "name").as[String]
      backends0         <- if ((c --\ "backends").succeeded)
                             (c --\ "backends").as[Map[String, OneOrMore[TestDirective]]]
                               .map(_ mapKeys (BackendName(_)))
                           else ok(Map[BackendName, OneOrMore[TestDirective]]())
      backends          =  backends0 mapValues (_.value)
      data              <- (c --\ "data").as[List[RelFile[Unsandboxed]]] |||
                           optional[RelFile[Unsandboxed]](c--\ "data").map(_.toList)
      query             <- (c --\ "query").as[String]
      variables         <- orElse(c --\ "variables", Map.empty[String, String])
      ignoredFields     <- orElse(c --\ "ignoredFields", List.empty[String])
      ignoreFieldOrder  <- orElse(c --\ "ignoreFieldOrder", false)
      ignoreResultOrder <- orElse(c --\ "ignoreResultOrder", false)
      rows              <- (c --\ "expected").as[List[Json]]
      predicate         <- (c --\ "predicate").as[Predicate]
    } yield RegressionTest(
      name, backends, data, query, variables,
      ExpectedResult(rows, predicate, ignoredFields, ignoreFieldOrder, ignoreResultOrder, backends)))
}
