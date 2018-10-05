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

package quasar.qscript.rewrites

import slamdata.Predef.{Map => _, _}
import quasar.RenderTreeT
import quasar.common.{SortDir, JoinType}
import quasar.contrib.matryoshka._
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski._
import quasar.qscript._
import quasar.qscript.RecFreeS._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import scalaz._, Scalaz._
import simulacrum.typeclass
import iotaz.{ CopK, TListK, TNilK }
import iotaz.TListK.:::

@typeclass trait Normalizable[F[_]] {
  def normalizeF: NTComp[F, Option]
}

// it would be nice to use the `NTComp` alias here, but it cannot compile
trait NormalizableInstances {
  import Normalizable._

  def normalizable[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] =
    new NormalizableT[T]

  implicit def const[A]: Normalizable[Const[A, ?]] =
    make(λ[Const[A, ?] ~> (Option ∘ Const[A, ?])#λ](_ => None))

  implicit def qscriptCore[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]
      : Normalizable[QScriptCore[T, ?]] =
    normalizable[T].QScriptCore

  implicit def projectBucket[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]
      : Normalizable[ProjectBucket[T, ?]] =
    normalizable[T].ProjectBucket

  implicit def thetaJoin[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]
      : Normalizable[ThetaJoin[T, ?]] =
    normalizable[T].ThetaJoin

  implicit def equiJoin[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]
      : Normalizable[EquiJoin[T, ?]] =
    normalizable[T].EquiJoin

  implicit def copk[LL <: TListK](implicit M: Materializer[LL]): Normalizable[CopK[LL, ?]] =
    M.materialize(offset = 0)

  sealed trait Materializer[LL <: TListK] {
    def materialize(offset: Int): Normalizable[CopK[LL, ?]]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[F[_]](
      implicit
      F: Normalizable[F]
    ): Materializer[F ::: TNilK] = new Materializer[F ::: TNilK] {
      override def materialize(offset: Int): Normalizable[CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new Normalizable[CopK[F ::: TNilK, ?]] {
          override def normalizeF: NTComp[CopK[F ::: TNilK, ?], Option] = new (CopK[F ::: TNilK, ?] ~> (Option ∘ CopK[F ::: TNilK, ?])#λ) {
            override def apply[A](cfa: CopK[F ::: TNilK, A]): Option[CopK[F ::: TNilK, A]] = cfa match {
              case I(fa) => F.normalizeF(fa).map(I(_))
            }
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[F[_], LL <: TListK](
      implicit
      F: Normalizable[F],
      LL: Materializer[LL]
    ): Materializer[F ::: LL] = new Materializer[F ::: LL] {
      override def materialize(offset: Int): Normalizable[CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new Normalizable[CopK[F ::: LL, ?]] {
          override def normalizeF: NTComp[CopK[F ::: LL, ?], Option] = new (CopK[F ::: LL, ?] ~> (Option ∘ CopK[F ::: LL, ?])#λ) {
            override def apply[A](cfa: CopK[F ::: LL, A]): Option[CopK[F ::: LL, A]] = cfa match {
              case I(fa) => F.normalizeF(fa).map(I(_))
              case other => LL.materialize(offset  + 1).normalizeF(other.asInstanceOf[CopK[LL, A]]).asInstanceOf[Option[CopK[F ::: LL, A]]]
            }
          }
        }
      }
    }
  }
}

class NormalizableT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] extends TTypes[T] {
  import Normalizable._
  lazy val rewrite = new Rewrite[T]

  // FIXME: Should this be a version that also normalizes EquiJoin
  def freeTC(free: FreeQS): FreeQS =
    free.transCata[FreeQS](liftCo(rewrite.normalizeTJCoEnv[QScriptTotal]))

  def freeTCEq(free: FreeQS): Option[FreeQS] = {
    val freeNormalized = freeTC(free)
    (free ≠ freeNormalized).option(freeNormalized)
  }

  def freeMFEq[A: Equal: Show](fm: Free[MapFunc, A]): Option[Free[MapFunc, A]] = {
    val fmNormalized = freeMF[A](fm)
    (fm ≠ fmNormalized).option(fmNormalized)
  }

  def freeMF[A: Equal: Show](fm: Free[MapFunc, A]): Free[MapFunc, A] =
    fm.transCata[Free[MapFunc, A]](MapFuncCore.normalize[T, A])

  def transformMFEq[A: Equal](fm: Free[MapFunc, A]): Option[Free[MapFunc, A]] = {
    val fmTransformed = transformMF[A](fm)
    (fm ≠ fmTransformed).option(fmTransformed)
  }

  def transformMF[A: Equal](fm: Free[MapFunc, A]): Free[MapFunc, A] =
    fm.transCata[Free[MapFunc, A]](MapFuncCore.transform[T, A])

  def makeNorm[A, B, C](
    lOrig: A, rOrig: B)(
    left: A => Option[A], right: B => Option[B])(
    f: (A, B) => C):
      Option[C] =
    (left(lOrig), right(rOrig)) match {
      case (None, None) => None
      case (l, r)       => f(l.getOrElse(lOrig), r.getOrElse(rOrig)).some
    }

  def EquiJoin = make(
    λ[EquiJoin ~> (Option ∘ EquiJoin)#λ](ej => {
      val keyOpt: List[(Option[FreeMap], Option[FreeMap])] =
        ej.key ∘ (_.bimap(freeMFEq[Hole], freeMFEq[Hole]))

      val keyNormOpt: Option[List[(FreeMap, FreeMap)]] =
        keyOpt.exists(p => p._1.nonEmpty || p._2.nonEmpty).option(
          Zip[List].zipWith(keyOpt, ej.key)((p1, p2) =>
            (p1._1.getOrElse(p2._1), p1._2.getOrElse(p2._2))))

      (freeTCEq(ej.lBranch), freeTCEq(ej.rBranch), keyNormOpt, freeMFEq(ej.combine)) match {
        case (None, None, None, None) => None
        case (lBranchNorm, rBranchNorm, keyNorm, combineNorm) =>
          quasar.qscript.EquiJoin(
            ej.src,
            lBranchNorm.getOrElse(ej.lBranch),
            rBranchNorm.getOrElse(ej.rBranch),
            keyNorm.getOrElse(ej.key),
            ej.f,
            combineNorm.getOrElse(ej.combine)).some
      }
    }))

  /** Extracts filter predicates from the join condition, pushing them out
    * to the branches and eliminates the same predicates from the combiner.
    */
  private def extractFilterFromThetaJoin[A](tj: ThetaJoin[A]): Option[ThetaJoin[A]] = {
    val lFilter = MapFuncCore.extractFilter(tj.on) {
      case LeftSide => SrcHole.some
      case RightSide => none
    }

    val rFilter = MapFuncCore.extractFilter(tj.on) {
        case LeftSide => none
        case RightSide => SrcHole.some
    }

    def simplifyCombine(f: JoinFunc => Option[JoinFunc]): JoinFunc =
      tj.combine.transApoT(c => f(c) <\/ c)

    (lFilter, rFilter) match {
      case (None, None) => none

      case (Some((lf, on, f)), _) =>
        some(quasar.qscript.ThetaJoin(
          tj.src,
          Free.roll(QCT(Filter(tj.lBranch, lf.asRec))),
          tj.rBranch,
          on,
          tj.f,
          simplifyCombine(f)))

      case (_, Some((rf, on, f))) =>
        some(quasar.qscript.ThetaJoin(
          tj.src,
          tj.lBranch,
          Free.roll(QCT(Filter(tj.rBranch, rf.asRec))),
          on,
          tj.f,
          simplifyCombine(f)))
    }
  }

  def ThetaJoin = make(
    λ[ThetaJoin ~> (Option ∘ ThetaJoin)#λ](tj => {
      def normalizeOn(j: JoinFunc, jt: JoinType): Option[JoinFunc] = jt match {
        case JoinType.Inner => freeMFEq(j)
        case _ => transformMFEq(j)
      }

      (freeTCEq(tj.lBranch), freeTCEq(tj.rBranch), normalizeOn(tj.on, tj.f), freeMFEq(tj.combine)) match {
        case (None, None, None, None) =>
          extractFilterFromThetaJoin(tj)

        case (lBranchNorm, rBranchNorm, onNorm, combineNorm) =>
          some(quasar.qscript.ThetaJoin(
            tj.src,
            lBranchNorm.getOrElse(tj.lBranch),
            rBranchNorm.getOrElse(tj.rBranch),
            onNorm.getOrElse(tj.on),
            tj.f,
            combineNorm.getOrElse(tj.combine)))
      }
    }))

  def rebucket(bucket: List[FreeMap]): Option[List[FreeMap]] = {
    val bucketNormOpt: List[Option[FreeMap]] = bucket ∘ freeMFEq[Hole]

    bucketNormOpt.any(_.isDefined).option(
      (Zip[List].zipWith(bucketNormOpt, bucket)(_.getOrElse(_)): List[FreeMap]) >>= (b =>
        b.resume.fold(
          {
            case MFC(MapFuncsCore.Constant(_)) => Nil
            case _                             => List(b)
          },
          κ(List(b)))))
  }

  def QScriptCore = {
    make(λ[QScriptCore ~> (Option ∘ QScriptCore)#λ] {
      case Reduce(src, bucket, reducers, repair) => {
        val reducersOpt: List[Option[ReduceFunc[FreeMap]]] =
          // FIXME: This shouldn’t use `traverse` because it does the wrong
          //        thing on UnshiftMap.
          reducers.map(_.traverse(freeMFEq[Hole](_)))

        val reducersNormOpt: Option[List[ReduceFunc[FreeMap]]] =
          reducersOpt.exists(_.nonEmpty).option(
            Zip[List].zipWith(reducersOpt, reducers)(_.getOrElse(_)))

        (rebucket(bucket), reducersNormOpt, freeMFEq(repair)) match {
          case (None, None, None) =>
            None
          case (bucketNorm, reducersNorm, repairNorm)  =>
            Reduce(
              src,
              bucketNorm.getOrElse(bucket),
              reducersNorm.getOrElse(reducers),
              repairNorm.getOrElse(repair)).some
        }
      }

      case Sort(src, bucket, order) =>
        val orderOpt: NonEmptyList[Option[(FreeMap, SortDir)]] =
          order.map { case (fm, dir) => freeMFEq(fm) strengthR dir }

        val orderNormOpt: Option[NonEmptyList[(FreeMap, SortDir)]] =
          orderOpt any (_.nonEmpty) option orderOpt.fzipWith(order)(_ | _)

        makeNorm(bucket, order)(rebucket, _ => orderNormOpt)(Sort(src, _, _))

      case Map(src, f)                   => freeMFEq(f.linearize).map(RecFreeS.fromFree(_)).map(Map(src, _))
      case LeftShift(src, s, i, t, u, r) => makeNorm(s.linearize, r)(freeMFEq(_), freeMFEq(_))((str, repair) => LeftShift(src, str.asRec, i, t, u, repair))
      case Union(src, l, r)              => makeNorm(l, r)(freeTCEq(_), freeTCEq(_))(Union(src, _, _))
      case Filter(src, f)                => freeMFEq(f.linearize).map(fm => Filter(src, fm.asRec))
      case Subset(src, from, sel, count) => makeNorm(from, count)(freeTCEq(_), freeTCEq(_))(Subset(src, _, sel, _))
      case Unreferenced()                => None
    })
  }

  def ProjectBucket = make(
    λ[ProjectBucket ~> (Option ∘ ProjectBucket)#λ] {
      case BucketKey(a, v, k) => makeNorm(v, k)(freeMFEq(_), freeMFEq(_))(BucketKey(a, _, _))
      case BucketIndex(a, v, i) => makeNorm(v, i)(freeMFEq(_), freeMFEq(_))(BucketIndex(a, _, _))
    }
  )
}

object Normalizable extends NormalizableInstances {
  def make[F[_]](f: NTComp[F, Option]): Normalizable[F] = new Normalizable[F] { val normalizeF = f }
}
