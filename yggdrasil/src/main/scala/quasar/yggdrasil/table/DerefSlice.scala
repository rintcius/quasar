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

package quasar.yggdrasil
package table

import quasar.precog.common._

object DerefSlice {
  def apply(source: Slice, derefBy: PartialFunction[Int, CPathNode]): Slice = {

    val forwardIndex: Map[CPathNode, Map[ColumnRef, Column]] =
      source.columns.foldLeft(Map.empty[CPathNode, Map[ColumnRef, Column]]) {
        case (acc, (ColumnRef(CPath(root, xs @ _*), ctype), col)) =>
          val resultRef = ColumnRef(CPath(xs: _*), ctype)
          // we know the combination of xs and ctype to be unique within root
          acc + (root -> (acc.getOrElse(root, Map()) + (resultRef -> col)))

        case (acc, (ColumnRef(CPath(), _), _)) => acc
      }

    val indexableArrays: Map[ColumnRef, HomogeneousArrayColumn[_]] =
      source.columns.collect {
        case (ColumnRef(CPath(CPathArray, xs @ _*), ctype), col: HomogeneousArrayColumn[_]) =>
          (ColumnRef(CPath(xs: _*), ctype), col)
      }.toMap

    def derefColumns(node: CPathNode): Option[Map[ColumnRef, Column]] = node match {
      case CPathIndex(i) if !indexableArrays.isEmpty =>
        Some((indexableArrays mapValues (_.select(i))) ++ forwardIndex.getOrElse(node, Map.empty))
      case _ => forwardIndex get node
    }

    val size = source.size

    val cols = source.columns.keySet.foldLeft(Map.empty[ColumnRef, Column]) {
      case (acc, ColumnRef(CPath(_, xs @ _ *), ctype)) =>
        val resultRef = ColumnRef(CPath(xs: _*), ctype)

        lazy val resultCol = ctype match {
          case CBoolean =>
            new BoolColumn {
              private var row0: Int           = -1
              private var refCol0: BoolColumn = _
              @inline private def refCol(row: Int): BoolColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[BoolColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CLong =>
            new LongColumn {
              private var row0: Int           = -1
              private var refCol0: LongColumn = _
              @inline private def refCol(row: Int): LongColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[LongColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CDouble =>
            new DoubleColumn {
              private var row0: Int             = -1
              private var refCol0: DoubleColumn = _
              @inline private def refCol(row: Int): DoubleColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[DoubleColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CNum =>
            new NumColumn {
              private var row0: Int          = -1
              private var refCol0: NumColumn = _
              @inline private def refCol(row: Int): NumColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[NumColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CString =>
            new StrColumn {
              private var row0: Int          = -1
              private var refCol0: StrColumn = _
              @inline private def refCol(row: Int): StrColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[StrColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case COffsetDateTime =>
            new OffsetDateTimeColumn {
              private var row0: Int           = -1
              private var refCol0: OffsetDateTimeColumn = _
              @inline private def refCol(row: Int): OffsetDateTimeColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[OffsetDateTimeColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case COffsetTime =>
            new OffsetTimeColumn {
              private var row0: Int           = -1
              private var refCol0: OffsetTimeColumn = _
              @inline private def refCol(row: Int): OffsetTimeColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[OffsetTimeColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case COffsetDate =>
            new OffsetDateColumn {
              private var row0: Int           = -1
              private var refCol0: OffsetDateColumn = _
              @inline private def refCol(row: Int): OffsetDateColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[OffsetDateColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CLocalDateTime =>
            new LocalDateTimeColumn {
              private var row0: Int           = -1
              private var refCol0: LocalDateTimeColumn = _
              @inline private def refCol(row: Int): LocalDateTimeColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[LocalDateTimeColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CLocalTime =>
            new LocalTimeColumn {
              private var row0: Int           = -1
              private var refCol0: LocalTimeColumn = _
              @inline private def refCol(row: Int): LocalTimeColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[LocalTimeColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CLocalDate =>
            new LocalDateColumn {
              private var row0: Int           = -1
              private var refCol0: LocalDateColumn = _
              @inline private def refCol(row: Int): LocalDateColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[LocalDateColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CInterval =>
            new IntervalColumn {
              private var row0: Int             = -1
              private var refCol0: IntervalColumn = _
              @inline private def refCol(row: Int): IntervalColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[IntervalColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case cArrayType: CArrayType[a] =>
            new HomogeneousArrayColumn[a] {
              val tpe = cArrayType
              private var row0: Int                          = -1
              private var refCol0: HomogeneousArrayColumn[a] = _
              @inline private def refCol(row: Int): HomogeneousArrayColumn[a] =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[HomogeneousArrayColumn[a]]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CEmptyObject =>
            new EmptyObjectColumn {
              def isDefinedAt(row: Int) =
                derefBy.isDefinedAt(row) &&
                  derefColumns(derefBy(row)).exists(_.get(resultRef).exists(_.isDefinedAt(row)))
            }

          case CEmptyArray =>
            new EmptyArrayColumn {
              def isDefinedAt(row: Int) =
                derefBy.isDefinedAt(row) &&
                  derefColumns(derefBy(row)).exists(_.get(resultRef).exists(_.isDefinedAt(row)))
            }

          case CNull =>
            new NullColumn {
              def isDefinedAt(row: Int) = derefBy.isDefinedAt(row) && derefColumns(derefBy(row)).exists(cols => cols(resultRef).isDefinedAt(row))
            }

          case CUndefined => UndefinedColumn.raw
        }

        acc + (resultRef -> acc.getOrElse(resultRef, resultCol))

      // deref at the root is eliminationd
      case (acc, ColumnRef(CPath(), _)) => acc
    }

    Slice(size, cols)
  }
}

/* A strict version
derefBy.columns.headOption collect {
  case c: StrColumn =>
    val transforms: Map[String, Map[ColumnRef, (Column, Column with ArrayColumn[_])]] =
      slice.columns.foldLeft(Map.empty[String, Map[ColumnRef, (Column, Column with ArrayColumn[_])]]) {
        case (acc, (ColumnRef(CPath(CPathField(root), xs @ _*), ctype), col)) =>
          val resultRef = ColumnRef(CPath(xs: _*), ctype)

          // find the result column if it exists, or else create a new one.
          val resultColumn = acc.get(root).flatMap(_.get(resultRef).map(_._2)).getOrElse(
            ctype match {
              case CBoolean     => ArrayBoolColumn()
              case CLong        => ArrayLongColumn(slice.size)
              case CDouble      => ArrayDoubleColumn(slice.size)
              case CNum         => ArrayNumColumn(slice.size)
              case CString      => ArrayStrColumn(slice.size)
              case CDate        => ArrayDateColumn(slice.size)
              case CEmptyObject => MutableEmptyObjectColumn()
              case CEmptyArray  => MutableEmptyArrayColumn()
              case CNull        => MutableNullColumn()
            }
          )

          val suffixes = acc.getOrElse(root, Map())
          // we know the combination of xs and ctype to be unique within root
          // the result column is shared between common occurrences of resultRef
          // across roots
          acc + (root -> (suffixes + (resultRef -> ((col, resultColumn)))))
      }

    for (i <- 0 until slice.size if c.isDefinedAt(i); cols <- transforms.get(c(i))) {
      cols foreach {
        case (_, (source: BoolColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[Boolean]](i) = source(i)

        case (_, (source: LongColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[Long]](i) = source(i)

        case (_, (source: DoubleColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[Double]](i) = source(i)

        case (_, (source: NumColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[BigDecimal]](i) = source(i)

        case (_, (source: StrColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[String]](i) = source(i)

        case (_, (source: DateColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[DateTime]](i) = source(i)

        case (_, (source: EmptyObjectColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[Boolean]](i) = source.isDefinedAt(i)

        case (_, (source: EmptyArrayColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[Boolean]](i) = source.isDefinedAt(i)

        case (_, (source: NullColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[Boolean]](i) = source.isDefinedAt(i)

        case _ =>
      }
    }

    // we can safely throw out any duplicated keys, since the values for duplicated
    // keys are all shared
    transforms.values map { _.mapValues(_._2) } reduceOption { _ ++ _ } getOrElse { Map() }
} getOrElse {
  slice.columns
}
 */
