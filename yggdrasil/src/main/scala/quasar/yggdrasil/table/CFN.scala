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

import quasar.precog.BitSet
import quasar.precog.common._
import qdata.time.{DateTimeInterval, OffsetDate}
import quasar.yggdrasil.bytecode.JType

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

import cats.effect.IO

sealed trait CFId
case class LeafCFId(identity: String)            extends CFId
case class ComposedCFId(l: CFId, r: CFId)        extends CFId
case class PartialLeftCFId(cv: CValue, r: CFId)  extends CFId
case class PartialRightCFId(l: CFId, cv: CValue) extends CFId

object CFId {
  def apply(identity: String) = LeafCFId(identity)
}

trait CF {
  def identity: CFId

  override final def equals(other: Any): Boolean = other match {
    case cf: CF => identity == cf.identity
    case _      => false
  }

  override final def hashCode: Int = identity.hashCode

  override def toString() = identity.toString
}

trait CF1 extends CF { self =>
  def apply(c: Column): Option[Column]
  def apply(cv: CValue): Option[Column] = apply(Column.const(cv))

  // Do not use PartialFunction.compose or PartialFunction.andThen for composition,
  // because they will fail out with MatchError.
  def compose(f1: CF1): CF1 = new CF1 {
    def apply(c: Column) = f1(c).flatMap(self.apply)
    val identity = ComposedCFId(f1.identity, self.identity)
  }

  def andThen(f1: CF1): CF1 = new CF1 {
    def apply(c: Column) = self.apply(c).flatMap(f1.apply)
    val identity = ComposedCFId(self.identity, f1.identity)
  }
}

object CF1 {
  def apply(name: String)(f: Column => Option[Column]): CF1 = apply(CFId(name))(f)
  def apply(id: CFId)(f: Column => Option[Column]): CF1 = new CF1 {
    def apply(c: Column) = f(c)
    val identity = id
  }
}

object CF1P {
  def apply(name: String)(f: PartialFunction[Column, Column]): CF1 = apply(CFId(name))(f)
  def apply(id: CFId)(f: PartialFunction[Column, Column]): CF1 = new CF1 {
    def apply(c: Column) = f.lift(c)
    val identity = id
  }
}

trait CF2 extends CF { self =>
  def apply(c1: Column, c2: Column): Option[Column]

  @inline
  def partialLeft(cv: CValue): CF1 = {
    new CF1 {
      def apply(c2: Column) = self.apply(Column.const(cv), c2)
      val identity = PartialLeftCFId(cv, self.identity)
    }
  }

  @inline
  def partialRight(cv: CValue): CF1 = {
    new CF1 {
      def apply(c1: Column) = self.apply(c1, Column.const(cv))
      val identity = PartialRightCFId(self.identity, cv)
    }
  }
}

object CF2 {
  def apply(id: String)(f: (Column, Column) => Option[Column]): CF2 = apply(CFId(id))(f)
  def apply(id: CFId)(f: (Column, Column) => Option[Column]): CF2 = new CF2 {
    def apply(c1: Column, c2: Column) = f(c1, c2)
    val identity = id
  }
}

object CF2P {
  def apply(id: String)(f: PartialFunction[(Column, Column), Column]): CF2 = apply(CFId(id))(f)
  def apply(id: CFId)(f: PartialFunction[(Column, Column), Column]): CF2 = new CF2 {
    def apply(c1: Column, c2: Column) = f.lift((c1, c2))
    val identity = id
  }
}

trait CFN extends CF {
  def apply(columns: List[Column]): Option[Column]
}

object CFN {
  def apply(id: String)(f: List[Column] => Option[Column]): CFN = apply(CFId(id))(f)
  def apply(id: CFId)(f: List[Column] => Option[Column]): CFN = new CFN {
    def apply(columns: List[Column]): Option[Column] = f(columns)
    val identity = id
  }
}

object CFNP {
  def apply(id: String)(f: PartialFunction[List[Column], Column]): CFN = apply(CFId(id))(f)
  def apply(id: CFId)(f: PartialFunction[List[Column], Column]): CFN = new CFN {
    private val lifted: List[Column] => Option[Column] = f.lift
    def apply(columns: List[Column]): Option[Column] = lifted(columns)
    val identity = id
  }
}

object CF2Array {
  def apply[A](name: String)(pf: PartialFunction[(Column, Column, Range), (CType, Array[Array[A]], BitSet)]): CMapper = new ArrayMapperS {
    def apply(columns0: Map[ColumnRef, Column], range: Range) = {
      for {
        (ColumnRef(CPath(CPathIndex(0)), _), col1) <- columns0
        (ColumnRef(CPath(CPathIndex(1)), _), col2) <- columns0
        if pf.isDefinedAt((col1, col2, range))
      } yield {
        val (tpe, cols, defined) = pf((col1, col2, range))
        tpe -> (cols.asInstanceOf[Array[Array[_]]] -> defined)
      }
    }
  }
}

trait CScanner {
  type A
  def init: A
  def scan(a: A, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column])
}

sealed trait CMapper {
  def fold[A](f: CMapperS => A, g: CMapperM => A): A
}

trait CMapperS extends CMapper {
  final def fold[A](f: CMapperS => A, g: CMapperM => A): A = f(this)

  def map(cols: Map[ColumnRef, Column], range: Range): Map[ColumnRef, Column]
}

trait CMapperM extends CMapper {
  final def fold[A](f: CMapperS => A, g: CMapperM => A): A = g(this)

  def map(cols: Map[ColumnRef, Column], range: Range): IO[Map[ColumnRef, Column]]
}

trait CSchema {
  def columnRefs: Set[ColumnRef]
  def columnMap(jtype: JType): Map[ColumnRef, Column]
  final def columns(jtype: JType): Set[Column] = columnMap(jtype).values.toSet
}

trait CReducer[A] {
  def reduce(schema: CSchema, range: Range): A
}

trait ArrayMapperS extends CMapperS {
  def map(columns0: Map[ColumnRef, Column], range: Range): Map[ColumnRef, Column] = {
    val results = this(columns0, range)

    val columns = results flatMap {
      case (tpe, (cols0, defined)) if tpe eq CString => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[String]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) ->
            new StrColumn {
              def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
              def apply(row: Int)       = cols(row)(i)
            }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq CBoolean => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[Boolean]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new BoolColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int)       = cols(row)(i)
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq CLong => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[Long]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new LongColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int)       = cols(row)(i)
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq CDouble => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[Double]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new DoubleColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int)       = cols(row)(i)
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq CNum => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[BigDecimal]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new NumColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int)       = cols(row)(i)
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq CNull => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[Unit]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new NullColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq CEmptyObject => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[Unit]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new EmptyObjectColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq CEmptyArray => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[Unit]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new EmptyArrayColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq COffsetDateTime => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[OffsetDateTime]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new OffsetDateTimeColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int)       = cols(row)(i)
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq COffsetTime => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[OffsetTime]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new OffsetTimeColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int)       = cols(row)(i)
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq COffsetDate => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[OffsetDate]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new OffsetDateColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int)       = cols(row)(i)
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq CLocalDateTime => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[LocalDateTime]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new LocalDateTimeColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int)       = cols(row)(i)
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq CLocalTime => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[LocalTime]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new LocalTimeColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int)       = cols(row)(i)
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq CLocalDate => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[LocalDate]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new LocalDateColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int)       = cols(row)(i)
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, (cols0, defined)) if tpe eq CInterval => {
        val max  = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[DateTimeInterval]]]

        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new IntervalColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int)       = cols(row)(i)
          }
        })(collection.breakOut)

        columns
      }

      case (tpe, _) => sys.error("Unsupported CFArray type: " + tpe)
    }

    columns
  }

  def apply(columns0: Map[ColumnRef, Column], range: Range): Map[CType, (Array[Array[_]], BitSet)]

  private[this] def maxIds(arr: Array[Array[_]], mask: BitSet): Int = {
    var back = -1
    0 until arr.length foreach { i =>
      if (mask get i) {
        back = back max arr(i).length
      }
    }
    back
  }
}
/* ctags
type FN */
