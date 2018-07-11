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
package table

import quasar.blueeyes._
import quasar.blueeyes.json._
import quasar.precog.common._
import quasar.precog.util._

import cats.effect.IO
import scalaz._
import scalaz.syntax.std.boolean._
import shims._

import scala.annotation.tailrec

trait ColumnarTableModuleTestSupport extends ColumnarTableModule with TableModuleTestSupport {
  def newGroupId: GroupId

  def defaultSliceSize = 10

  private def makeSlice(sampleData: Stream[JValue], sliceSize: Int): (Slice, Stream[JValue]) = {
    @tailrec def buildColArrays(from: Stream[JValue], into: Map[ColumnRef, ArrayColumn[_]], sliceIndex: Int): (Map[ColumnRef, ArrayColumn[_]], Int) = {
      from match {
        case jv #:: xs =>
          val refs = Slice.withIdsAndValues(jv, into, sliceIndex, sliceSize)
          buildColArrays(xs, refs, sliceIndex + 1)
        case _ =>
          (into, sliceIndex)
      }
    }

    val (prefix, suffix) = sampleData.splitAt(sliceSize)
    val slice = new Slice {
      val (columns, size) = buildColArrays(prefix.toStream, Map.empty[ColumnRef, ArrayColumn[_]], 0)
    }

    (slice, suffix)
  }

  // production-path code uses fromRValues, but all the tests use fromJson
  // this will need to be changed when our tests support non-json such as CDate and CPeriod
  def fromJson0(values: Stream[JValue], maxSliceRows: Option[Int] = None): Table = {
    val sliceSize = maxSliceRows.getOrElse(Config.maxSliceRows)

    Table(
      StreamT.unfoldM(values) { events =>
        IO {
          (!events.isEmpty) option {
            makeSlice(events.toStream, sliceSize)
          }
        }
      },
      ExactSize(values.length)
    )
  }

  def fromJson(values: Stream[JValue], maxSliceRows: Option[Int] = None): Table =
    fromJson0(values, maxSliceRows orElse Some(defaultSliceSize))

  def lookupF1(namespace: List[String], name: String): F1 = {
    val lib = Map[String, CF1](
      "negate" -> cf.math.Negate,
      "coerceToDouble" -> cf.util.CoerceToDouble,
      "true" -> CF1 { _ => Some(Column.const(true)) }
    )

    lib(name)
  }

  def lookupF2(namespace: List[String], name: String): F2 = {
    val lib  = Map[String, CF2](
      "add" -> cf.math.Add,
      "mod" -> cf.math.Mod,
      "eq"  -> cf.std.Eq
    )
    lib(name)
  }

  def lookupScanner(namespace: List[String], name: String): CScanner = {
    val lib = Map[String, CScanner](
      "sum" -> new CScanner {
        type A = BigDecimal
        val init = BigDecimal(0)
        def scan(a: BigDecimal, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
          val identityPath = cols collect { case c @ (ColumnRef(CPath.Identity, _), _) => c }
          val prioritized = identityPath.values filter {
            case (_: LongColumn | _: DoubleColumn | _: NumColumn) => true
            case _ => false
          }

          val mask = BitSetUtil.filteredRange(range.start, range.end) {
            i => prioritized exists { _ isDefinedAt i }
          }

          val (a2, arr) = mask.toList.foldLeft((a, new Array[BigDecimal](range.end))) {
            case ((acc, arr), i) => {
              val col = prioritized find { _ isDefinedAt i }

              val acc2 = col map {
                case lc: LongColumn   => acc + lc(i)
                case dc: DoubleColumn => acc + dc(i)
                case nc: NumColumn    => acc + nc(i)
                case _                => abort("unreachable")
              }

              acc2 foreach { arr(i) = _ }

              (acc2 getOrElse acc, arr)
            }
          }

          (a2, Map(ColumnRef(CPath.Identity, CNum) -> ArrayNumColumn(mask, arr)))
        }
      }
    )

    lib(name)
  }
}
