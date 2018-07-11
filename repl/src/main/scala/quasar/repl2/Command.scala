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
import quasar.contrib.pathy._
import quasar.repl._
import quasar.sql.{Query}

import eu.timepit.refined.auto._
import pathy.Path, Path._
import scalaz._, Scalaz._

sealed abstract class Command

object Command {
  private val NamePattern                  = "[a-zA-Z0-9-]+"

  private val ExitPattern                  = "(?i)(?:exit)|(?:quit)".r
  private val HelpPattern                  = "(?i)(?:help)|(?:commands)|\\?".r
  private val CdPattern                    = "(?i)cd(?: +(.+))?".r
  private val NamedExprPattern             = "(?i)([^ :]+) *<- *(.+)".r
  private val ExplainPattern               = "(?i)explain +(.+)".r
  private val CompilePattern               = "(?i)compile +(.+)".r
  private val LsPattern                    = "(?i)ls(?: +(.+))?".r
  private val SetPhaseFormatPattern        = "(?i)(?:set +)?phaseFormat *= *(tree|code)".r
  private val SetTimingFormatPattern       = "(?i)(?:set +)?timingFormat *= *(tree|onlytotal)".r
  private val DebugPattern                 = "(?i)(?:set +)?debug *= *(0|1|2)".r
  private val SummaryCountPattern          = "(?i)(?:set +)?summaryCount *= *(\\d+)".r
  private val FormatPattern                = "(?i)(?:set +)?format *= *((?:table)|(?:precise)|(?:readable)|(?:csv))".r
  private val SetVarPattern                = "(?i)(?:set +)?(\\w+) *= *(.*\\S)".r
  private val UnsetVarPattern              = "(?i)unset +(\\w+)".r
  private val ListVarPattern               = "(?i)env".r
  private val DataSourcesPattern           = "(?i)datasources".r
  private val DataSourceTypesPattern       = "(?i)types".r
  private val DataSourceAddPattern         = s"(?i)(?:add +)($NamePattern)(?: +)($NamePattern)(?: +)(replace|preserve)(?: +)(.*\\S)".r
  private val DataSourceLookupPattern      = "(?i)get +([\\S]+)".r
  private val DataSourceRemovePattern      = "(?i)rm +([\\S]+)".r

  final case object Exit extends Command
  final case object Help extends Command
  final case object ListVars extends Command
  final case class Cd(dir: XDir) extends Command
  final case class Select(name: Option[String], query: Query) extends Command
  final case class Explain(query: Query) extends Command
  final case class Compile(query: Query) extends Command
  final case class Ls(dir: Option[XDir]) extends Command
  final case class Debug(level: DebugLevel) extends Command
  final case class SummaryCount(rows: Int) extends Command
  final case class Format(format: OutputFormat) extends Command
  final case class SetPhaseFormat(format: PhaseFormat) extends Command
  final case class SetTimingFormat(format: TimingFormat) extends Command
  final case class SetVar(name: String, value: String) extends Command
  final case class UnsetVar(name: String) extends Command

  final case object DataSources extends Command
  final case object DataSourceTypes extends Command
  final case class DataSourceLookup(name: ResourceName) extends Command
  final case class DataSourceAdd(name: ResourceName, tp: DataSourceType.Name, config: String, onConflict: ConflictResolution) extends Command
  //final case class DataSourceMove
  final case class DataSourceRemove(name: ResourceName) extends Command

  implicit val equalCommand: Equal[Command] = Equal.equalA

  def parse(input: String): Command =
    input match {
      case ExitPattern()                            => Exit
      case CdPattern(XDir(d))                       => Cd(d)
      case CdPattern(_)                             => Cd(rootDir.right)
      case NamedExprPattern(name, query)            => Select(Some(name), Query(query))
      case ExplainPattern(query)                    => Explain(Query(query))
      case CompilePattern(query)                    => Compile(Query(query))
      case LsPattern(XDir(d))                       => Ls(d.some)
      case LsPattern(_)                             => Ls(none)
      case DebugPattern(code)                       => Debug(DebugLevel.int.unapply(code.toInt) | DebugLevel.Normal)
      case SetPhaseFormatPattern(format)            => SetPhaseFormat(PhaseFormat.fromString(format) | PhaseFormat.Tree)
      case SetTimingFormatPattern(format)           => SetTimingFormat(TimingFormat.fromString(format) | TimingFormat.OnlyTotal)
      case SummaryCountPattern(rows)                => SummaryCount(rows.toInt)
      case FormatPattern(format)                    => Format(OutputFormat.fromString(format) | OutputFormat.Table)
      case HelpPattern()                            => Help
      case SetVarPattern(name, value)               => SetVar(name, value)
      case UnsetVarPattern(name)                    => UnsetVar(name)
      case ListVarPattern()                         => ListVars
      case DataSourcesPattern()                     => DataSources
      case DataSourceTypesPattern()                 => DataSourceTypes
      case DataSourceLookupPattern(n)               => DataSourceLookup(ResourceName(n))
      case DataSourceAddPattern(n, DataSourceType.string(tp), onConflict, cfg) =>
                                                       DataSourceAdd(ResourceName(n), tp, cfg,
                                                         ConflictResolution.string.getOption(onConflict) | ConflictResolution.Preserve)
      case DataSourceRemovePattern(n)               => DataSourceRemove(ResourceName(n))
      case _                                        => Select(None, Query(input))
    }

  type XDir = RelDir[Unsandboxed] \/ ADir
  object XDir {
    def unapply(str: String): Option[XDir] =
      Option(str)
        .filter(_ ≠ "")
        .map(s => if (s.endsWith("/")) s else s + "/")
        .flatMap { s =>
          posixCodec.parseRelDir(s).map(_.left) orElse
            posixCodec.parseAbsDir(s).map(unsafeSandboxAbs(_).right)
        }
  }
  type XFile = RelFile[Unsandboxed] \/ AFile
  object XFile {
    def unapply(str: String): Option[XFile] =
      Option(str)
        .filter(_ ≠ "")
        .flatMap { s =>
          posixCodec.parseRelFile(s).map(_.left) orElse
            posixCodec.parseAbsFile(s).map(unsafeSandboxAbs(_).right)
        }
  }
}
