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

package quasar.api.table

import slamdata.Predef.Unit

import quasar.Condition

import fs2.Stream
import scalaz.\/

/** @tparam I identity
  * @tparam Q query type
  * @tparam D materialized table data
  */
trait Tables[F[_], I, Q, D] {
  import TableError.{
    ExistenceError,
    ModificationError,
    NameConflict,
    PreparationNotInProgress,
    PrePreparationError
  }

  def allTables: Stream[F, (I, TableRef[Q], PreparationStatus)]

  def table(tableId: I): F[ExistenceError[I] \/ TableRef[Q]]

  def createTable(table: TableRef[Q]): F[NameConflict \/ I]

  def replaceTable(tableId: I, table: TableRef[Q]): F[Condition[ModificationError[I]]]

  def prepareTable(tableId: I): F[Condition[PrePreparationError[I]]]

  def preparationEvents: Stream[F, PreparationEvent[I]]

  def preparationStatus(tableId: I): F[ExistenceError[I] \/ PreparationStatus]

  def cancelPreparation(tableId: I): F[Condition[PreparationNotInProgress[I]]]

  def cancelAllPreparations: F[Unit]

  def preparedData(tableId: I): F[ExistenceError[I] \/ PreparationResult[I, D]]
}
