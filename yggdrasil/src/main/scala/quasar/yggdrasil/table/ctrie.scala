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
import com.rklaehn.sonicreducer.Reducer
import scalaz._, Scalaz._
import shims._

object ctrie {

  type CTrie = RadixTree[Array[CPathNode], Map[CType, Column]]

  def empty: RadixTree[Array[CPathNode], Map[CType, Column]] = RadixTree.empty

  def nrColumns(trie: CTrie): Int = (Reducer.reduce(trie.values.map(_.size))(_ + _)).getOrElse(0)

  def fromLegacy(map: Map[ColumnRef, Column]): CTrie = {
    val l: List[(List[CPathNode], CType, Column)] =
      map.toList.map(p => (p._1.selector.nodes, p._1.ctype, p._2))
    val m: Map[List[CPathNode], Map[CType, Column]] =
      l.groupBy(_._1).mapValues(ts => ts.map(t => (t._2, t._3)).toMap)
    RadixTree(m.toList.map { case (k, v) => (k.toArray, v)} :_*)
  }

  def toLegacy(trie: CTrie): Map[ColumnRef, Column] = {
    def toList(pathNodes: Array[CPathNode], map: Map[CType, Column]): List[(ColumnRef, Column)] =
      map.toList.map { case (tp, c) => (ColumnRef(CPath(pathNodes.toList), tp), c) }

    trie.entries.map(e => toList(e._1, e._2)).flatten.toMap
  }
}
