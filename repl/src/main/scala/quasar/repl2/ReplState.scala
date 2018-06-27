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

package quasar.repl2

import slamdata.Predef._

import quasar.contrib.pathy._
import quasar.fp.numeric.widenPositive
import quasar.repl._
import quasar.repl2.Command._

import eu.timepit.refined.refineMV
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import pathy.Path, Path._
import scalaz._, Scalaz._

 final case class ReplState(
    cwd:                ADir,
    debugLevel:         DebugLevel,
    phaseFormat:        PhaseFormat,
    summaryCount:       Option[Int Refined Positive],
    format:             OutputFormat,
    variables:          Map[String, String],
    timingFormat:       TimingFormat
  ) {

  def targetDir(path: Option[XDir]): ADir =
    path match {
      case None          => cwd
      case Some( \/-(a)) => a
      case Some(-\/ (r)) =>
        (unsandbox(cwd) </> r)
          .relativeTo(rootDir[Sandboxed])
          .cata(rootDir </> _, rootDir)
    }

  def targetFile(path: XFile): AFile =
    path match {
      case  \/-(a) => a
      case -\/ (r) =>
        (unsandbox(cwd) </> r)
          .relativeTo(rootDir[Sandboxed])
          .cata(rootDir </> _, rootDir </> file1(fileName(r)))
    }
}

object ReplState {
  def mk: ReplState = ReplState(
    rootDir,
    DebugLevel.Normal,
    PhaseFormat.Tree,
    refineMV[Positive](10).some,
    OutputFormat.Table,
    Map(),
    TimingFormat.OnlyTotal
  )
}
