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

package quasar.mimir

import slamdata.Predef.{IndexedSeq, Int}
import quasar.blueeyes.json.JValue
import quasar.yggdrasil.table.Slice

import scalaz.syntax.bifunctor._
import scalaz.std.tuple._

final class SliceIndexedSeq private (slice: Slice) extends IndexedSeq[JValue] {
  val length: Int = slice.size

  def apply(idx: Int): JValue =
    slice.toJValue(idx)

  override def drop(n: Int): IndexedSeq[JValue] =
    SliceIndexedSeq(slice.drop(n))

  override def take(n: Int): IndexedSeq[JValue] =
    SliceIndexedSeq(slice.take(n))

  override def splitAt(n: Int): (IndexedSeq[JValue], IndexedSeq[JValue]) =
    slice.split(n).umap(SliceIndexedSeq(_))
}

object SliceIndexedSeq {
  def apply(slice: Slice): IndexedSeq[JValue] =
    new SliceIndexedSeq(slice)
}
