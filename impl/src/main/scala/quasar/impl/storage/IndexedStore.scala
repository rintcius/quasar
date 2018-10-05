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

package quasar.impl.storage

import slamdata.Predef.{Boolean, Option, Unit}
import quasar.higher.HFunctor

import cats.arrow.FunctionK
import fs2.Stream
import scalaz.{~>, Bind, Functor, InvariantFunctor, Monad, Scalaz}, Scalaz._

trait IndexedStore[F[_], I, V] {
  /** All values in the store paired with their index. */
  def entries: Stream[F, (I, V)]

  /** Retrieve the value at the specified index. */
  def lookup(i: I): F[Option[V]]

  /** Associate the given value with the specified index, replaces any
    * existing association.
    */
  def insert(i: I, v: V): F[Unit]

  /** Remove any value associated with the specified index, returning whether
    * it existed.
    */
  def delete(i: I): F[Boolean]
}

object IndexedStore extends IndexedStoreInstances {
  /** Transform the index of a store. */
  def xmapIndex[F[_]: Functor, I, V, J](
      s: IndexedStore[F, I, V])(
      f: I => J)(
      g: J => I)
      : IndexedStore[F, J, V] =
    new IndexedStore[F, J, V] {
      def entries: Stream[F, (J, V)] =
        s.entries.map(_.leftMap(f))

      def lookup(j: J): F[Option[V]] =
        s.lookup(g(j))

      def insert(j: J, v: V): F[Unit] =
        s.insert(g(j), v)

      def delete(j: J): F[Boolean] =
        s.delete(g(j))
    }

  /** Effectfully transform the index of a store. */
  def xmapIndexF[F[_], I, V, J](
      s: IndexedStore[F, I, V])(
      f: I => F[J])(
      g: J => F[I])(
      implicit F: Bind[F])
      : IndexedStore[F, J, V] =
    new IndexedStore[F, J, V] {
      def entries: Stream[F, (J, V)] =
        s.entries flatMap {
          case (i, v) => Stream.eval(F.map(f(i))((_, v)))
        }

      def lookup(j: J): F[Option[V]] =
        F.bind(g(j))(s.lookup)

      def insert(j: J, v: V): F[Unit] =
        F.bind(g(j))(s.insert(_, v))

      def delete(j: J): F[Boolean] =
        F.bind(g(j))(s.delete)
    }

  /** Effectfully transform the value of a store. */
  def xmapValueF[F[_], I, A, B](
      s: IndexedStore[F, I, A])(
      f: A => F[B])(
      g: B => F[A])(
      implicit F: Monad[F])
      : IndexedStore[F, I, B] =
    new IndexedStore[F, I, B] {
      def entries: Stream[F, (I, B)] =
        s.entries flatMap {
          case (i, a) => Stream.eval(F.map(f(a))((i, _)))
        }

      def lookup(i: I): F[Option[B]] =
        F.bind(s.lookup(i))(_.traverse(f))

      def insert(i: I, v: B): F[Unit] =
        F.bind(g(v))(s.insert(i, _))

      def delete(i: I): F[Boolean] =
        s.delete(i)
    }
}

sealed abstract class IndexedStoreInstances {
  implicit def hFunctor[I, V]: HFunctor[IndexedStore[?[_], I, V]] =
    new HFunctor[IndexedStore[?[_], I, V]] {
      def hmap[A[_], B[_]](fa: IndexedStore[A, I, V])(f: A ~> B): IndexedStore[B, I, V] =
        new IndexedStore[B, I, V] {
          def entries: Stream[B, (I, V)] =
            fa.entries.translate(λ[FunctionK[A, B]](f(_)))

          def lookup(i: I): B[Option[V]] =
            f(fa.lookup(i))

          def insert(i: I, v: V): B[Unit] =
            f(fa.insert(i, v))

          def delete(i: I): B[Boolean] =
            f(fa.delete(i))
        }
    }

  implicit def valueInvariantFunctor[F[_]: Functor, I]: InvariantFunctor[IndexedStore[F, I, ?]] =
    new InvariantFunctor[IndexedStore[F, I, ?]] {
      def xmap[A, B](fa: IndexedStore[F, I, A], f: A => B, g: B => A): IndexedStore[F, I, B] =
        new IndexedStore[F, I, B] {
          def entries: Stream[F, (I, B)] =
            fa.entries.map(_.map(f))

          def lookup(i: I): F[Option[B]] =
            fa.lookup(i).map(_.map(f))

          def insert(i: I, v: B): F[Unit] =
            fa.insert(i, g(v))

          def delete(i: I): F[Boolean] =
            fa.delete(i)
        }
    }
}
