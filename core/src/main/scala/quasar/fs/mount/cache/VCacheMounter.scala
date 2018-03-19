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

package quasar.fs.mount.cache

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.effect.{KeyValueStore, Timing}
import quasar.fp.ski._
import quasar.frontend.{SemanticErrors, logicalplan => lp}, lp.{LogicalPlan => LP}
import quasar.fs.FileSystemError
import quasar.fs.mount._, Mounting._, ViewCache.Status
import quasar.sql.{ScopedExpr, Sql}

import java.time.Instant
import scala.concurrent.duration.Duration

import eu.timepit.refined.auto._
import matryoshka.data.Fix
import pathy.Path, Path._
import scalaz._, Scalaz._

object VCacheMounter {

  def apply[S[_]](implicit
    M: Mounting :<: S,
    S1: VCache.VCacheKVS :<: S,
    S2: VCache.VCacheMkR :<: S,
    T: Timing.Ops[S],
  ): Mounting ~> Free[S, ?] = {

    implicit val mount = Mounting.Ops[S]
    implicit val CR = VCache.VCacheMkR.Ops[S]

    def vcache =
      KeyValueStore.Ops[AFile, ViewCache, S]

    def askCacheMk(path: APath): Free[S, Option[(Duration, AFile)]] =
      CR.ask.map(_.get(path))

    def deleteCache(path: APath): Free[S, Unit] =
      (OptionT(refineType(path).toOption.η[Free[S, ?]]) >>= (f =>
        vcache.get(f) >> (vcache.delete(f).liftM[OptionT]))).run.void

    def reresolveCaches: Free[S, Unit] =
      for {
        vcs <- vcaches
        ts <- T.timestamp
        res <- vcs.map { case (f, vc) => reresolveCache(f, vc, ts) }.sequence.void
      } yield res

    def reresolveCache(file: AFile, vc: ViewCache, refreshAfter: Instant): Free[S, Unit] = {
      for {
        err <- calcErr(vc.viewConfig)
        updated = (vc.status, err) match {
          case (Status.Failed, none) =>
            ViewCache.repair(vc, refreshAfter).some
          case (s, Some(e)) if IList[Status](Status.Successful, Status.Pending).element(s) =>
            ViewCache.fail(vc, s"Failed to resolve query. Details: ${e.show}").some
          case x =>
            none
        }
        mod <- updated match {
          case Some(u) => vcache.modify(file, κ(u))
          case None => Free.point[S, Unit](())
        }
      } yield mod
    }

    def vcaches: Free[S, IList[(AFile, ViewCache)]] = {
      val keys: Free[S, IList[AFile]] = vcache.keys.map(v => IList.fromList(v.toList))

      def convertEntry(p: (AFile, Option[ViewCache])): IList[(AFile, ViewCache)] =
        IList.fromOption(p._2.map((p._1, _)))

      keys.map(_.traverse(k => (Free.point[S, AFile](k) ⊛ vcache.get(k).run).tupled.map(convertEntry))).join.map(_.join)
    }

    def resolve(lp: Fix[LP]): Free[S, Option[FileSystemError]] =
      view.resolveViewRefs[S](lp).run.map(_.swap.toOption)

    def calcErr(config: MountConfig.ViewConfig): Free[S, Option[SemanticErrors \/ FileSystemError]] =
      quasar.queryPlan(config.query.expr, config.vars, pathy.Path.rootDir, 0L, None)
        .run.run._2.traverse(resolve).map(_.fold(-\/(_).some, x => x.map(\/-(_))))

    def mkCache(file: AFile, viewConfig: MountConfig.ViewConfig): Free[S, Option[ViewCache]] =
      ((OptionT(askCacheMk(file)) ⊛ OptionT(T.timestamp.map(_.some)))
       ((df, ts) => ViewCache.mk(viewConfig, df._1.toSeconds, ts, df._2))).run

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def vc(query: ScopedExpr[Fix[Sql]], vars: quasar.Variables): MountConfig.ViewConfig =
      MountConfig.viewConfig(query, vars).asInstanceOf[MountConfig.ViewConfig]

    def writeCache(file: AFile, viewConfig: MountConfig.ViewConfig) =
      for {
        newViewCache <- OptionT(mkCache(file, viewConfig))
        writeStatus <- OptionT(writeVCache(file, newViewCache).map(_.some))
      } yield writeStatus

    sealed trait WriteStatus
    case object Unchanged extends WriteStatus
    case object Updated extends WriteStatus
    case object New extends WriteStatus

    def writeVCache(file: AFile, newViewCache: ViewCache): Free[S, WriteStatus] =
      vcache.alterS(file, alterViewCache(newViewCache))

    def alterViewCache(nw: ViewCache)(old: Option[ViewCache]): (ViewCache, WriteStatus) =
      old match {
        case None => (nw, New)
        case Some(o) =>
          if (MountConfig.equal.equal(nw.viewConfig, o.viewConfig))
            // only update maxAgeSeconds and refreshAfter (relative to maxAgeSeconds)
            (o.copy(
              maxAgeSeconds = nw.maxAgeSeconds,
              refreshAfter = o.refreshAfter.plusSeconds(nw.maxAgeSeconds - o.maxAgeSeconds)), Unchanged)
          else
            (nw, Updated)
      }

    new (Mounting ~> Free[S, ?]) {
      def apply[A](m: Mounting[A]): Free[S, A] = m match {
        case h@HavingPrefix(dir) => mount.lift(h)

        case l@LookupType(path) => mount.lift(l)

        case l@LookupConfig(path) => mount.lift(l)

        case m@MountView(loc, query, vars) => mount.lift(m) <* (writeCache(loc, vc(query, vars)).run *> reresolveCaches)

        case m@MountFileSystem(loc, typ, uri) => mount.lift(m) <* reresolveCaches

        case m@MountModule(loc, statements) => mount.lift(m) <* reresolveCaches

        case u@Unmount(path) => mount.lift(u) <* (deleteCache(path) *> reresolveCaches)

        case r@Remount(src, dst) => mount.lift(r) <* reresolveCaches
      }
    }
  }
}
