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
import quasar.fp.numeric.widenPositive
import quasar.repl._

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import scalaz._, Scalaz._

 final case class ReplState(
    cwd:                ResourcePath,
    debugLevel:         DebugLevel,
    phaseFormat:        PhaseFormat,
    summaryCount:       Option[Int Refined Positive],
    format:             OutputFormat,
    variables:          Variables,
    timingFormat:       TimingFormat,
    supportedTypes:     Option[ISet[DataSourceType]]
  )

object ReplState {


  def mk: ReplState = ReplState(
    ResourcePath.Root,
    DebugLevel.Normal,
    PhaseFormat.Tree,
    Some(10),
    OutputFormat.Table,
    Variables(Map()),
    TimingFormat.OnlyTotal,
    none
  )
}
