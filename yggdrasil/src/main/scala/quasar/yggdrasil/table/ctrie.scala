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

package quasar.yggdrasil.table

import quasar.precog.common._

import com.rklaehn.radixtree.RadixTree
import scalaz._, Scalaz._
import shims._

object ctrie {

  type CTrie = RadixTree[Array[CPathNode], Column]
  type CMap = Map[CType, CTrie]

  def empty: RadixTree[Array[CPathNode], Column] = RadixTree.empty

  def nrColumns(cmap: CMap): Int = cmap.values.map(_.count).foldLeft(0)(_ + _)

  def fromLegacy(map: Map[ColumnRef, Column]): CMap = {
    val l: List[(CType, (Array[CPathNode], Column))] =
      map.toList.map(p => (p._1.ctype, (p._1.selector.nodes.toArray, p._2)))
    l.groupBy(_._1).mapValues(p => RadixTree(p.map(_._2): _*))
  }

  def toLegacy(cmap: CMap): Map[ColumnRef, Column] = {
    def toMap(ctype: CType, trie: CTrie): Map[ColumnRef, Column] =
      trie.entries.map { case (k, v) => (ColumnRef(CPath(k.toList), ctype), v) }.toMap

    val l: List[Map[ColumnRef, Column]] = cmap.toList.map(p => toMap(p._1, p._2))
    l.foldLeft[Map[ColumnRef, Column]](Map.empty) { case (acc, m) => acc ++ m }
  }

}
