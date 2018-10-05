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

package quasar.sst

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.ejson.{EJson, CommonEJson => C, Str}
import quasar.fp.numeric._
import quasar.contrib.iota.copkTraverse
import quasar.tpe._

import matryoshka.{project => _, _}
import matryoshka.patterns.EnvT
import matryoshka.implicits._
import scalaz._, Scalaz._
import spire.algebra.{AdditiveSemigroup, Field}
import spire.math.ConvertableTo

object compression {
  import StructuralType.{ConstST, TypeST, TagST, isConst}, ExtractPrimary.ops._

  /** Compress a map having greater than `maxSize` keys by moving the largest
    * group of keys having the same `PrimaryTag` to the unknown key field
    * and their values to the unknown value field.
    *
    * @param maxSize threshold at which a map is considered for key compression
    * @param retainMostFrequent the number of keys, in desc order of
    *                           frequency, to retain from a compression set
    */
  def coalesceKeys[J: Order, A: Order: Field: ConvertableTo](
      maxSize: Natural,
      retainMostFrequent: Natural)(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : SSTF[J, A, SST[J, A]] => Option[SSTF[J, A, SST[J, A]]] = {
    case EnvT((ts, TypeST(TypeF.Map(kn, unk)))) if kn.size > maxSize.value =>
      val pruned =
        groupByPrimaryTag(kn)
          .maximumBy(_.size)
          .map(withoutTopValues(_, retainMostFrequent.value.toInt))

      val compressed = pruned map { m =>
        val toCompress = kn intersection m
        (kn \\ toCompress, compressMap(toCompress) |+| unk)
      }

      compressed map {
        case (kn1, unk1) => envT(ts, TypeST(TypeF.map[J, SST[J, A]](kn1, unk1)))
      }

    case _ => none
  }

  /** Compress unions by combining any constants with their primary type if it
    * also appears in the union.
    */
  def coalescePrimary[J: Order, A: Order: Field: ConvertableTo](
      stringPreserveStructure: Boolean)(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : SSTF[J, A, SST[J, A]] => Option[SSTF[J, A, SST[J, A]]] = {
    case EnvT((ts, TypeST(TypeF.Unioned(xs)))) if willCoalesce(xs) =>
      val grouped = xs.list groupBy { sst =>
        isConst(sst).fold(none, sst.project.primaryTag)
      }

      grouped.minView flatMap { case (nonPrimary, m0) =>
        val coalesced = nonPrimary.foldLeft(m0 map (_.suml1)) { (m, sst) =>
          sst.project.primaryTag flatMap { pt =>
            m.member(some(pt)) option m.adjust(some(pt), _ |+| widenConst(stringPreserveStructure)(sst))
          } getOrElse m.updateAppend(none, sst)
        }
        coalesced.suml1Opt map (csst => envT(ts, csst.project.lower))
      }

    case _ => none
  }

  /** Compress maps having unknown keys by coalescing known keys with unknown
    * when the unknown contains any values having the same `PrimaryTag` as the
    * known key.
    *
    * @param retainMostFrequent the number of keys to retain from a compression set
    */
  def coalesceWithUnknown[J: Order, A: Order: Field: ConvertableTo](
      retainMostFrequent: Natural)(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : SSTF[J, A, SST[J, A]] => Option[SSTF[J, A, SST[J, A]]] = {
    case EnvT((ts, TypeST(TypeF.Map(kn, Some((k, v)))))) =>
      val unkPrimaries = k.project.lower match {
        case TypeST(TypeF.Unioned(ts)) =>
          ISet.fromFoldable[NelOpt, PrimaryTag](ts map (_.project.primaryTag))

        case tpe =>
          ISet.fromFoldable(tpe.primaryTag)
      }

      val pruned =
        groupByPrimaryTag(kn).foldlWithKey(IMap.empty[J, A]) { (r, t, m) =>
          if (unkPrimaries member t)
            r union withoutTopValues(m, retainMostFrequent.value.toInt)
          else
            r
        }

      val (toCompress, unchanged) = kn partitionWithKey { (j, _) =>
        pruned member j
      }

      compressMap(toCompress) map { compressed =>
        envT(ts, TypeST(TypeF.map(unchanged, some(compressed |+| ((k, v))))))
      }

    case _ => none
  }

  /** Replace statically known arrays longer than the given limit with a lub array. */
  def limitArrays[J: Order, A: Order](maxLength: Natural, retainCount: Natural)(
      implicit
      A : Field[A],
      JR: Recursive.Aux[J, EJson])
      : ElgotCoalgebra[SST[J, A] \/ ?, SSTF[J, A, ?], SST[J, A]] =
    sst => sst.project match {
      case EnvT((_, TagST(Tagged(strings.StructuralString, _)))) =>
        sst.left

      case EnvT((ts, TypeST(TypeF.Arr(elts, u)))) if elts.length > maxLength.value =>
        val (ret, lim) = elts.splitAt(retainCount.value.toInt)
        envT(ts, TypeST(TypeF.arr[J, SST[J, A]](ret, lim.suml1Opt |+| u))).right

      case other =>
        other.right
    }

  /** Replace literal string types longer than the given limit with `char[]`
    * or `SimpleType.String` if `preserveStructure` is `false`. */
  def limitStrings[J, A: ConvertableTo: Field: Order](
      maxLength: Natural,
      preserveStructure: Boolean)(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : SSTF[J, A, SST[J, A]] => Option[SSTF[J, A, SST[J, A]]] =
    _.some collect {
      case EnvT((ts, TypeST(TypeF.Const(Embed(C(Str(s))))))) if s.length > maxLength.value =>
        if (preserveStructure)
          strings.compress[SST[J, A], J, A](ts, s)
        else
          strings.simple[SST[J, A], J, A](ts)
    }

  /** Compress a union larger than `maxSize` by reducing the largest group of
    * values sharing a primary type to their shared type.
    */
  def narrowUnion[J: Order, A: Order: Field: ConvertableTo](
      maxSize: Positive,
      stringPreserveStructure: Boolean)(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : SSTF[J, A, SST[J, A]] => Option[SSTF[J, A, SST[J, A]]] = {
    case EnvT((ts, TypeST(TypeF.Unioned(xs)))) if xs.length > maxSize.value =>
      val grouped = xs.list groupBy (_.project.primaryTag)

      val compressed = (grouped - none).toList.maximumBy(_._2.length) map {
        case (pt, ssts) =>
          val compress1 = ssts.foldMap1(widenConst[J, A](stringPreserveStructure)).wrapNel
          grouped.insert(pt, compress1)
      }

      compressed flatMap (_.foldMap(_.list).toNel) collect {
        case NonEmptyList(x, ICons(y, zs)) if (zs.length + 2) < xs.length =>
          envT(ts, TypeST(TypeF.union[J, SST[J, A]](x, y, zs)))

        case NonEmptyList(x, INil()) =>
          envT(ts, x.project.lower)
      }

    case _ => none
  }

  /** Returns the SST of the primary tag of the given EJson value. */
  def primarySst[J: Order, A: ConvertableTo: Field: Order](
      stringPreserveStructure: Boolean)(
      cnt: A, j: J)(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : SST[J, A] = j match {
    case Embed(C(Str(s))) if stringPreserveStructure =>
      strings.widen[J, A](cnt, s).embed

    case Embed(C(Str(_))) =>
      strings.simple[SST[J, A], J, A](TypeStat.fromEJson(cnt, j)).embed

    case SimpleEJson(s) =>
      envT(TypeStat.fromEJson(cnt, j), TypeST(TypeF.simple[J, SST[J, A]](s))).embed

    case _ => SST.fromEJson(cnt, j)
  }

  /** Returns the primary SST if the argument is a constant, otherwise returns
    * the argument itself.
    */
  def widenConst[J: Order, A: ConvertableTo: Field: Order](
      stringPreserveStructure: Boolean)(
      sst: SST[J, A])(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : SST[J, A] = {
    def psst(ts: TypeStat[A], j: J): SST[J, A] =
      StructuralType.measure[J, TypeStat[A]]
        .set(ts)(primarySst(stringPreserveStructure)(ts.size, j))

    sst.project match {
      case ConstST(None, ts, j) =>
        psst(ts, j)

      case ConstST(Some((ts0, t)), ts, j) =>
        envT(ts0, TagST[J](Tagged(t, psst(ts, j)))).embed

      case _ => sst
    }
  }

  ////

  private type NelOpt[A] = NonEmptyList[Option[A]]
  private implicit val foldableNelOpt: Foldable[NelOpt] = Foldable[NonEmptyList].compose[Option]

  private val emptyTags: IMap[PrimaryTag, Int] = IMap.empty

  private def compressMap[J: Order, A: Order: Field: ConvertableTo](
      m: J ==>> SST[J, A])(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : Option[(SST[J, A], SST[J, A])] =
    m.foldlWithKey(none[(SST[J, A], SST[J, A])]) { (r, j, sst) =>
      r |+| some((SST.fromEJson(SST.size(sst), j), sst))
    }

  private def groupByPrimaryTag[J: Order, A: AdditiveSemigroup](
      kn: IMap[J, SST[J, A]])(
      implicit
      JR: Recursive.Aux[J, EJson])
      : IMap[PrimaryTag, J ==>> A] =
    kn.foldlWithKey(IMap.empty[PrimaryTag, J ==>> A]) { (m, j, s) =>
      m.alter(
        primaryTagOf(j),
        _ map (_ insert (j, SST.size(s))) orElse some(IMap.singleton(j, SST.size(s))))
    }

  private def willCoalesce[F[_]: Foldable, J, A](
      fa: F[SST[J, A]])(
      implicit
      J: Recursive.Aux[J, EJson])
      : Boolean = {
    val (consts, nonConsts) =
      fa.foldMap(sst =>
        isConst(sst).fold(
          (IMap.fromFoldable(sst.project.primaryTag strengthR 1), emptyTags),
          (emptyTags, IMap.fromFoldable(sst.project.primaryTag strengthR 1))))

    !consts.intersection(nonConsts).isEmpty || nonConsts.any(_ > 1)
  }

  private def withoutTopValues[K: Order, V: Order](m: IMap[K, V], k: Int): IMap[K, V] =
    if (k > 0) IMap.fromList(m.toList.sortWith((x, y) => y._2 < x._2).drop(k)) else m
}
