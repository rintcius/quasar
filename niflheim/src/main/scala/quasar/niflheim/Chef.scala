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

package quasar.niflheim

import quasar.precog.common._

import java.io._
import java.nio.channels.WritableByteChannel

import akka.actor._

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.std.list._

final case class Prepare(blockid: Long, seqId: Long, root: File, source: StorageReader, onComplete: () => Unit)
final case class PrepareSegments(blockid: Long, seqId: Long, root: File, offset: Long, length: Int, segments: List[Segment], onComplete: () => Unit)
final case class Spoilt(blockid: Long, seqId: Long, onComplete: () => Unit)
final case class Cooked(blockid: Long, seqId: Long, root: File, metadata: File, onComplete: () => Unit)

final case class Chef(blockFormat: CookedBlockFormat, format: SegmentFormat) extends Actor {
  private def typeCode(ctype: CType): String = CType.nameOf(ctype)

  def prefix(id: Segment): String = {
    val pathHash = id.cpath.hashCode.toString
    "segment-" + id.blockid + "-" + pathHash + "-" + typeCode(id.ctype)
  }

  def cookSegments(root: File, id: Long, length: Int, segments: List[Segment]): ValidationNel[IOException, File] ={
    assert(root.exists)
    assert(root.isDirectory)
    assert(root.canWrite)

    val files = segments traverse { seg =>
      val file = File.createTempFile(prefix(seg), ".cooked", root)
      val relativized = new File(file.getName)
      val channel: WritableByteChannel = new FileOutputStream(file).getChannel()
      val result = try {
        format.writer.writeSegment(channel, seg).map(_ => (seg.id, relativized))
      } finally {
        channel.close()
      }
      result.toValidationNel
    }

    files flatMap { segs =>
      val metadata = CookedBlockMetadata(id, length, segs.toArray)
      val mdFile = File.createTempFile("block-%08x".format(id), ".cookedmeta", root)
      val channel = new FileOutputStream(mdFile).getChannel()
      try {
        blockFormat.writeCookedBlock(channel, metadata).toValidationNel map { _: Unit =>
          new File(mdFile.getName)
        }
      } finally {
        channel.close()
      }
    }
  }

  def cook(root: File, reader: StorageReader): ValidationNel[IOException, File] =
    cookSegments(root, reader.id, reader.length, reader.snapshot(None).segments.toList)

  def receive = {
    case Prepare(blockid, seqId, root, source, onComplete) =>
      cook(root, source) match {
        case Success(file) =>
          sender ! Cooked(blockid, seqId, root, file, onComplete)
        case Failure(_) =>
          sender ! Spoilt(blockid, seqId, onComplete)
      }

    case PrepareSegments(blockid, seqId, root, offset, length, segments, onComplete) =>
      cookSegments(root, offset, length, segments) match {
        case Success(file) =>
          sender ! Cooked(blockid, seqId, root, file, onComplete)
        case Failure(_) =>
          sender ! Spoilt(blockid, seqId, onComplete)
      }
  }
}

