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

import quasar.blueeyes._, json._
import quasar.RCValueGenerators
import quasar.precog.common._

import scalaz._, Scalaz._

import scala.collection.generic.CanBuildFrom
import scala.util.Random

import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._
import SJValueGenerators.JSchema

case class SampleData(data: Stream[RValue], schema: Option[(Int, JSchema)] = None) {
  override def toString = {
    "SampleData: \ndata = "+data.map(_.toString.replaceAll("\n", "\n  ")).mkString("[\n  ", ",\n  ", "]\n") +
    "\nschema: " + schema
  }

  def sortBy[B: scala.math.Ordering](f: JValue => B) =
    copy(data = data.sortBy(f compose (_.toJValue)))
}

object SampleData extends SJValueGenerators with RCValueGenerators {
  def apply(data: Stream[JValue]): SampleData =
    new SampleData(data.flatMap(RValue.fromJValue), None)

  def toRecord(ids: Array[Long], jv: JValue): JValue =
    JObject(Nil).set(JPath(".key"), JArray(ids.map(JNum(_)).toList)).set(JPath(".value"), jv)

  implicit def keyOrder[A]: scala.math.Ordering[(Identities, A)] =
    tupledIdentitiesOrder[A](IdentitiesOrder).toScalaOrdering

  def sample(schema: Int => Gen[JSchema]): Arbitrary[SampleData] = Arbitrary(
    for {
      depth   <- choose(0, 1)
      jschema <- schema(depth)
      (idCount, data) <- genEventColumns(jschema)
    } yield {
      SampleData(
        data.sorted flatMap {
          // Sometimes the assembly process will generate overlapping values which will
          // cause RuntimeExceptions in JValue.unsafeInsert. It's easier to filter these
          // out here than prevent it from happening in the first place.
          case (ids, jv) => try { Some(RValue.fromJValueRaw(toRecord(ids, assemble(jv)))) } catch { case _ : RuntimeException => None }
        },
        Some((idCount, jschema)))
    })

  def randomSubset[T, C[X] <: Seq[X], S](c: C[T], freq: Double)(implicit cbf: CanBuildFrom[C[T], T, C[T]]): C[T] = {
    val builder = cbf()

    for (t <- c)
      if (Random.nextDouble < freq)
        builder += t

    builder.result
  }

  def sort(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(sampleData.data.sortBy(_.toJValue), sampleData.schema)
      })
  }

  def distinct(sample: Arbitrary[SampleData]) : Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(sampleData.data.distinct, sampleData.schema)
      })
  }

  def duplicateRows(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    val gen =
      for {
        sampleData <- arbitrary(sample)
      } yield {
        val rows = sampleData.data
        val duplicates = randomSubset(rows, 0.25)
        SampleData(Random.shuffle(rows ++ duplicates), sampleData.schema)
      }

    Arbitrary(gen)
  }

  def undefineRows(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    val gen =
      for {
        sampleData <- arbitrary(sample)
      } yield {
        val rows = for(row <- sampleData.data)
          yield if (Random.nextDouble < 0.25) CUndefined else row
        SampleData(rows, sampleData.schema)
      }

    Arbitrary(gen)
  }

  def undefineRowsForColumn(sample: Arbitrary[SampleData], path: JPath): Arbitrary[SampleData] = {
    val gen = for {
      sampleData <- arbitrary(sample)
    } yield {
      val rows = for (row <- sampleData.data) yield {
        val rowj = row.toJValue
        val rp = rowj.get(path)
        if (rp != null && rp != JUndefined) {
          rowj.set(path, JUndefined)
        } else {
          rowj
        }
      }
      SampleData(rows.flatMap(RValue.fromJValue), sampleData.schema)
    }

    Arbitrary(gen)
  }
}
