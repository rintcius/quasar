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

package quasar.api

import slamdata.Predef.{ -> => _, _ }
import quasar.fs._, mount.cache.VCache._

import scala.concurrent.duration.Duration

import org.http4s.CacheDirective.`max-age`
import org.http4s.dsl._
import org.http4s.headers.`Cache-Control`
import scalaz._, Scalaz._

object VCacheMiddleware {
  def apply[S[_]](
    service: QHttpService[S]
  )(implicit
    MF: ManageFile.Ops[S],
    CW: VCacheMkW.Ops[S]
  ): QHttpService[S] =
    QHttpService {
      case req @ PUT -> AsPath(path) =>

        val maxAge: Option[Duration] = req.headers.get(`Cache-Control`).flatMap(
          _.values.list.collectFirst { case `max-age`(d) => d})

        val cacheMk = for {
          d <- OptionT(Free.point[S, Option[Duration]](maxAge))
          f <- OptionT(MF.tempFile(path).run.map(_.toOption))
          r <- OptionT(CW.tell(Map(path -> ((d, f)))).map(_.some))
        } yield r

        services.respond(cacheMk.run *> service(req))
    }
}
