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

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{ReadableByteChannel, WritableByteChannel}

import scalaz.{Validation, Success, Failure}

trait Versioning {
  def magic: Short
  def version: Short

  def writeVersion(channel: WritableByteChannel): Validation[IOException, Unit] = {
    val buffer = ByteBuffer.allocate(4)
    buffer.putShort(magic)
    buffer.putShort(version)
    buffer.flip()

    try {
      while (buffer.remaining() > 0) {
        channel.write(buffer)
      }
      Success(())
    } catch { case ioe: IOException =>
      Failure(ioe)
    }
  }

  def readVersion(channel: ReadableByteChannel): Validation[IOException, Int] = {
    val buffer = ByteBuffer.allocate(4)
    try {
      while (buffer.remaining() > 0) {
        channel.read(buffer)
      }
      buffer.flip()

      val magic0: Short = buffer.getShort()
      val version0: Int = buffer.getShort()
      if (magic0 == magic) {
        Success(version0)
      } else {
        Failure(new IOException("Incorrect magic number found."))
      }
    } catch { case ioe: IOException =>
      Failure(ioe)
    }
  }
}


