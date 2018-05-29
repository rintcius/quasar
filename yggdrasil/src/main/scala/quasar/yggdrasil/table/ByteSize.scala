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

object ByteSize {
  // Byte sizes per row for an ArrayColumn with associated type

  // NB: most of these are not exact (at all). At this stage they are just meant
  // as an improvement over counting the number of rows.
  // Since there's also a cost to calculating the exact sizes, we need a clearer
  // picture on the pros and cons of doing so.

  // see https://www.javamex.com/tutorials/memory/object_memory_usage.shtml
  // for info how we can do more exact calculations
  
  val long = 8
  val double = 8

  val boolean = 0
  val null_ = 0
  val emptyObject = 0
  val emptyArray = 0
  val undefined = 0

  // see https://stackoverflow.com/a/2501339/1320169
  val bigDecimal = 100
  val string = 100

  val offsetDateTime = 100
  val offsetDate = 100
  val offsetTime = 100
  val localDateTime = 100
  val localTime = 100
  val localDate = 100
  val dateTimeInterval = 100

  val array = 1000

  def fromRValue(value: RValue): Long = value match {
    case RObject(fields) => fields.values.map(fromRValue).foldLeft(0L)(_ + _)
    case RArray(elements) => elements.map(fromRValue).foldLeft(0L)(_ + _)
    case CArray(_, _) => ByteSize.array
    case CString(_) => ByteSize.string
    case CBoolean(_) => ByteSize.boolean
    case CLong(_) => ByteSize.long
    case CDouble(_) => ByteSize.double
    case CNum(_) => ByteSize.bigDecimal
    case COffsetDateTime(_) => ByteSize.offsetDateTime
    case COffsetDate(_) => ByteSize.offsetDate
    case COffsetTime(_) => ByteSize.offsetTime
    case CLocalDateTime(_) => ByteSize.localDateTime
    case CLocalTime(_) => ByteSize.localTime
    case CLocalDate(_) => ByteSize.localDate
    case CInterval(_) => ByteSize.dateTimeInterval
    case CNull => ByteSize.null_
    case CEmptyObject => ByteSize.emptyObject
    case CEmptyArray => ByteSize.emptyArray
    case CUndefined => ByteSize.undefined
  }
}
