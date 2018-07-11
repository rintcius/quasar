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

package quasar.yggdrasil

import quasar.blueeyes.json._
import quasar.pkg.tests._
import quasar.precog.common._

import cats.effect.IO

trait CrossSpec extends TableModuleTestSupport with SpecificationLike with ScalaCheck {
  import SampleData._
  import trans._

  def testCross(l: SampleData, r: SampleData) = {
    val ltable = fromSample(l)
    val rtable = fromSample(r)

    def removeUndefined(jv: JValue): JValue = jv match {
      case JObjectFields(jfields) => JObject(jfields collect { case JField(s, v) if v != JUndefined => JField(s, removeUndefined(v)) })
      case JArray(jvs)            => JArray(jvs map { jv => removeUndefined(jv) })
      case v                      => v
    }

    val expected: Stream[RValue] = for {
      lv <- l.data
      rv <- r.data
    } yield {
      RValue.fromJValueRaw(JObject(JField("left", removeUndefined(lv.toJValueRaw)) :: JField("right", removeUndefined(rv.toJValueRaw)) :: Nil))
    }

    val result = ltable.cross(rtable)(
      InnerObjectConcat(WrapObject(Leaf(SourceLeft), "left"), WrapObject(Leaf(SourceRight), "right"))
    )

    val jsonResult: IO[Stream[RValue]] = toJson(result)
    jsonResult.unsafeRunSync must_== expected
  }

  def testSimpleCross = {
    val s1 = SampleData(Stream(toRecord(Array(1), JParser.parseUnsafe("""{"a":[]}""")), toRecord(Array(2), JParser.parseUnsafe("""{"a":[]}"""))))
    val s2 = SampleData(Stream(toRecord(Array(1), JParser.parseUnsafe("""{"b":0}""")), toRecord(Array(2), JParser.parseUnsafe("""{"b":1}"""))))

    testCross(s1, s2)
  }

  def testCrossSingles = {
    val s1 = SampleData(Stream(
      toRecord(Array(1), JParser.parseUnsafe("""{ "a": 1 }""")),
      toRecord(Array(2), JParser.parseUnsafe("""{ "a": 2 }""")),
      toRecord(Array(3), JParser.parseUnsafe("""{ "a": 3 }""")),
      toRecord(Array(4), JParser.parseUnsafe("""{ "a": 4 }""")),
      toRecord(Array(5), JParser.parseUnsafe("""{ "a": 5 }""")),
      toRecord(Array(6), JParser.parseUnsafe("""{ "a": 6 }""")),
      toRecord(Array(7), JParser.parseUnsafe("""{ "a": 7 }""")),
      toRecord(Array(8), JParser.parseUnsafe("""{ "a": 8 }""")),
      toRecord(Array(9), JParser.parseUnsafe("""{ "a": 9 }""")),
      toRecord(Array(10), JParser.parseUnsafe("""{ "a": 10 }""")),
      toRecord(Array(11), JParser.parseUnsafe("""{ "a": 11 }"""))
    ))

    val s2 = SampleData(Stream(
      toRecord(Array(1), JParser.parseUnsafe("""{"b":1}""")),
      toRecord(Array(2), JParser.parseUnsafe("""{"b":2}"""))))

    testCross(s1, s2)
    testCross(s2, s1)
  }
}

