/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.couchbase.fs

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.{Data, DataCodec}
import quasar.effect.{KeyValueStore, MonotonicSeq, Read, UUID}
import quasar.fp.free._
import quasar.fs._
import quasar.physical.couchbase.common._

import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.transcoder.JsonTranscoder
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object writefile {
  import WriteFile._

  implicit val codec = DataCodec.Precise

  final case class State(bucket: Bucket, collection: String)

  def interpret[S[_]](
    implicit
    S0: KeyValueStore[WriteHandle, State, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Read[Context, ?] :<:  S,
    S3: UUID :<: S,
    S4: Task :<: S
  ): WriteFile ~> Free[S, ?] = λ[WriteFile ~> Free[S, ?]] {
    case Open(file)     => open(file)
    case Write(h, data) => write(h, data)
    case Close(h)       => close(h)
  }

  def writeHandles[S[_]](
    implicit
    S0: KeyValueStore[WriteHandle, State, ?] :<: S
  ) = KeyValueStore.Ops[WriteHandle, State, S]

  val jsonTranscoder = new JsonTranscoder

  def open[S[_]](
    file: AFile
  )(implicit
    S0: KeyValueStore[WriteHandle, State, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, FileSystemError \/ WriteHandle] =
    (for {
      ctx      <- context.ask.liftM[FileSystemErrT]
      bktCol   <- EitherT(bucketCollectionFromPath(file).point[Free[S, ?]])
      _        <- EitherT(lift(
                    Task.delay(ctx.manager.hasBucket(bktCol.bucket).booleanValue).ifM(
                      Task.now(().right),
                      Task.now(FileSystemError.pathErr(PathError.pathNotFound(file)).left)
                  )).into)
      bkt      <- lift(Task.delay(
                    ctx.cluster.openBucket(bktCol.bucket)
                  )).into.liftM[FileSystemErrT]
      i        <- MonotonicSeq.Ops[S].next.liftM[FileSystemErrT]
      handle   =  WriteHandle(file, i)
      _        <- writeHandles.put(handle, State(bkt, bktCol.collection)).liftM[FileSystemErrT]
    } yield handle).run

  def write[S[_]](
    h: WriteHandle,
    chunk: Vector[Data]
  )(implicit
    S0: KeyValueStore[WriteHandle, State, ?] :<: S,
    S1: Task :<: S,
    uuid: UUID.Ops[S]
  ): Free[S, Vector[FileSystemError]] =
    (for {
      st   <- writeHandles.get(h).toRight(Vector(FileSystemError.unknownWriteHandle(h)))
      data <- EitherT(lift(Task.delay(chunk.foldMap(d =>
                DataCodec.render(d).bimap(
                  err => Vector(FileSystemError.writeFailed(d, err.shows)),
                  str => Vector({
                    JsonObject
                      .create()
                      .put("type", st.collection)
                      .put("value", jsonTranscoder.stringToJsonObject(str))
                  }))
              ))).into)
      docs <- data.traverse(d =>
               uuid.randomUUID.map(k => JsonDocument.create(k.toString, d))
              ).liftM[EitherT[?[_], Vector[FileSystemError], ?]]
      _    <- lift(docs.traverse(d =>
                Task.delay(st.bucket.insert(d))
              )).into.liftM[EitherT[?[_], Vector[FileSystemError], ?]]
    } yield Vector.empty).merge

  def close[S[_]](
    h: WriteHandle
  )(implicit
    S0: KeyValueStore[WriteHandle, State, ?] :<: S,
    S1: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, Unit] =
    writeHandles.delete(h)

}
