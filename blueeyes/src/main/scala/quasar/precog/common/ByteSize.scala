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

package quasar.precog.common

object ByteSize {
  // see https://www.javamex.com/tutorials/memory/object_memory_usage.shtml
  val boolean = 1
  val long = 8
  val double = 8

  val null_ = 0
  val emptyObject = 0
  val emptyArray = 0
  val undefined = 0

  // see https://stackoverflow.com/a/2501339/1320169
  val bigDecimal = 100
  val string = 100

  val array = 1000

  val offsetDateTime = 100
  val offsetDate = 100
  val offsetTime = 100
  val localDateTime = 100
  val localTime = 100
  val localDate = 100
  val dateTimeInterval = 100
}
