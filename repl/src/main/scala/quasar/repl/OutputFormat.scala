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

package quasar.repl

import slamdata.Predef._

sealed abstract class OutputFormat
object OutputFormat {
  case object Table extends OutputFormat
  case object Precise extends OutputFormat
  case object Readable extends OutputFormat
  case object Csv extends OutputFormat
  case object HomogeneousCsv extends OutputFormat

  def fromString(str: String): Option[OutputFormat] =
    Some(str.toLowerCase) collect {
      case "table" => Table
      case "precise" => Precise
      case "readable" => Readable
      case "csv" => Csv
      case "homogeneouscsv" => HomogeneousCsv
    }

  def headerLines(format: OutputFormat): Int =
    format match {
      case Table => 2
      case Precise => 0
      case Readable => 0
      case Csv => 1
      case HomogeneousCsv => 1
    }
}
