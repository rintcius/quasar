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

import scalaz._, Scalaz._

class CsvSpec extends quasar.Qspec {
  import CsvDetect._

  val Standard =
    """a,b,c
      |1,2,3
      |4,5,6
      |"1,000","1 000","This is ""standard"" CSV."""".stripMargin
  val TSV =
    "a\tb\tc\n" +
    "1\t2\t3\n" +
    "4\t5\t6\n" +
    "1,000\t\"1 000\"\t\"This is \"\"TSV\"\", so-called.\""

  "testParse" should {
    "standard" in {
      val p = CsvParser.AllParsers(0)

      testParse(p, Standard) must beSome(TestResult(3, List(3, 3, 3, 0, 0, 0, 0, 0, 0, 0)))
    }
  }

  "bestParse" should {
    def parse(text: String) = bestParse(CsvParser.AllParsers)(text).map(_.toList.sequence).join

    "parse standard format" in {
      parse(Standard) must_==
        \/-(List(
          Record(List("a", "b", "c")),
          Record(List("1", "2", "3")),
          Record(List("4", "5", "6")),
          Record(List("1,000", "1 000", "This is \"standard\" CSV."))))
    }

    "parse TSV format" in {
      parse(TSV) must_==
        \/-(List(
          Record(List("a", "b", "c")),
          Record(List("1", "2", "3")),
          Record(List("4", "5", "6")),
          Record(List("1,000", "1 000", "This is \"TSV\", so-called."))))
    }

    "parse more standard format" in {
      // From https://en.wikipedia.org/wiki/Comma-separated_values
      parse("""Year,Make,Model,Length
              |1997,Ford,E350,2.34
              |2000,Mercury,Cougar,2.38""".stripMargin) must_==
        \/-(List(
          Record(List("Year", "Make", "Model", "Length")),
          Record(List("1997", "Ford", "E350", "2.34")),
          Record(List("2000", "Mercury", "Cougar", "2.38"))))
    }

    "parse semi-colons and decimal commas" in {
      // From https://en.wikipedia.org/wiki/Comma-separated_values
      parse("""Year;Make;Model;Length
              |1997;Ford;E350;2,34
              |2000;Mercury;Cougar;2,38""".stripMargin) must_==
        \/-(List(
          Record(List("Year", "Make", "Model", "Length")),
          Record(List("1997", "Ford", "E350", "2,34")),
          Record(List("2000", "Mercury", "Cougar", "2,38"))))
    }
  }
}
