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

package quasar.mimir

import quasar.precog.BitSet
import quasar.precog.common._
import quasar.std.StringLib
import quasar.yggdrasil.bytecode._

import quasar.yggdrasil.table._
import java.util.regex.{ Pattern, PatternSyntaxException }

/* DEPRECATED
 *
 * Currently, we are accepting StrColumn and DateColumn when we need
 * strings. This is because we don't have date literal syntax, so ingest
 * will turn some strings into dates (when they match ISO8601). But
 * in cases where users do want to use that data as a string, we must
 * allow that to occur.
 *
 * In the future when we have date literals for ingest, we should
 * revert this and only accept JTextT (StrColumn).
 */

trait StringLibModule extends ColumnarTableLibModule {
  trait StringLib extends ColumnarTableLib {
    import StdLib._

    override def _lib1 = super._lib1 ++ Set(length, trim, toUpperCase, toLowerCase, isEmpty, intern, parseNum, numToString)

    override def _lib2 =
      super._lib2 ++ Set(
        equalsIgnoreCase,
        codePointAt,
        startsWith,
        lastIndexOf,
        concat,
        endsWith,
        codePointBefore,
        takeLeft,
        takeRight,
        dropLeft,
        dropRight,
        matches,
        regexMatch,
        compare,
        compareIgnoreCase,
        equals,
        indexOf,
        split,
        splitRegex)

    class Op1SS(name: String, f: String => String) extends Op1F1 {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = UnaryOperationType(JTextT, JNumberT)
      def f1: F1 = CF1P {
        case c: StrColumn => new StrFrom.S(c, _ != null, f)
      }
    }

    class Op1SB(name: String, f: String => Boolean) extends Op1F1 {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = UnaryOperationType(JTextT, JNumberT)
      private def build(c: StrColumn) = new BoolFrom.S(c, _ != null, f)
      def f1: F1 = CF1P {
        case c: StrColumn => build(c)
      }
    }

    object trim extends Op1SS("trim", _.trim)

    object toUpperCase extends Op1SS("toUpperCase", _.toUpperCase)

    object toLowerCase extends Op1SS("toLowerCase", _.toLowerCase)

    object intern extends Op1SS("intern", _.intern)

    object readBoolean extends Op1F1 {
      val tpe = UnaryOperationType(JTextT, JBooleanT)
      def f1: F1 = CF1P {
        case c: StrColumn =>
          new BoolFrom.S(c, s => s == "true" || s == "false", _.toBoolean)
      }
    }

    object readInteger extends Op1F1 {
      val tpe = UnaryOperationType(JTextT, JNumberT)
      def f1: F1 = CF1P {
        case c: StrColumn =>
          new LongFrom.S(c, s => try { s.toLong; true } catch { case _: Exception => false }, _.toLong)
      }
    }

    object readDecimal extends Op1F1 {
      val tpe = UnaryOperationType(JTextT, JNumberT)
      def f1: F1 = CF1P {
        case c: StrColumn =>
          new NumFrom.S(c, s => try { BigDecimal(s); true } catch { case _: Exception => false }, BigDecimal(_))
      }
    }

    object readNull extends Op1F1 {
      val tpe = UnaryOperationType(JTextT, JNullT)
      def f1: F1 = CF1P {
        case c: StrColumn =>
          new NullColumn {
            def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) == "null"
          }
      }
    }

    object convertToString extends Op1F1 {
      val tpe = UnaryOperationType(JType.JPrimitiveUnfixedT, JTextT)
      def f1: F1 = CF1P {
        case c: BoolColumn =>
          new StrColumn {
            def apply(row: Int) = c(row).toString
            def isDefinedAt(row: Int) = c.isDefinedAt(row)
          }
        case c: LongColumn =>
          new StrColumn {
            def apply(row: Int) = c(row).toString
            def isDefinedAt(row: Int) = c.isDefinedAt(row)
          }
        case c: DoubleColumn =>
          new StrColumn {
            def apply(row: Int) = c(row).toString
            def isDefinedAt(row: Int) = c.isDefinedAt(row)
          }
        case c: NumColumn =>
          new StrColumn {
            def apply(row: Int) = c(row).toString
            def isDefinedAt(row: Int) = c.isDefinedAt(row)
          }
        case c: StrColumn =>
          new StrColumn {
            def apply(row: Int) = c(row).toString
            def isDefinedAt(row: Int) = c.isDefinedAt(row)
          }
        case c: NullColumn =>
          new StrColumn {
            def apply(row: Int) = "null"
            def isDefinedAt(row: Int) = c.isDefinedAt(row)
          }
      }
    }

    object isEmpty extends Op1F1 {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = UnaryOperationType(JTextT, JBooleanT)
      def f1: F1 = CF1P {
        case c: StrColumn  => new BoolFrom.S(c, _ != null, _.isEmpty)
      }
    }

    def neitherNull(x: String, y: String) = x != null && y != null

    object length extends Op1F1 {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = UnaryOperationType(JTextT, JNumberT)
      private def build(c: StrColumn) = new LongFrom.S(c, _ != null, _.length)
      def f1: F1 = CF1P {
        case c: StrColumn => build(c)
      }
    }

    class Op2SSB(name: String, f: (String, String) => Boolean) extends Op2F2 {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(JTextT, JTextT, JBooleanT)
      private def build(c1: StrColumn, c2: StrColumn) = new BoolFrom.SS(c1, c2, neitherNull, f)
      def f2: F2 = CF2P {
        case (c1: StrColumn, c2: StrColumn) => build(c1, c2)
      }
    }

    // FIXME: I think it's a bad idea to override Object.equals here...
    object equals extends Op2SSB("equals", _ equals _)

    object equalsIgnoreCase extends Op2SSB("equalsIgnoreCase", _ equalsIgnoreCase _)

    object startsWith extends Op2SSB("startsWith", _ startsWith _)

    object endsWith extends Op2SSB("endsWith", _ endsWith _)

    object matches extends Op2SSB("matches", _ matches _)

    // like matches, except for quasar, and hilariously less efficient
    lazy val searchDynamic: CFN = CFNP {
      case List(target: StrColumn, pattern: StrColumn, flag: BoolColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            // we're literally recompiling this on a row-by-row basis
            val compiled = if (flag(row))
              Pattern.compile(pattern(row), Pattern.CASE_INSENSITIVE)
            else
              Pattern.compile(pattern(row))

            compiled.matcher(target(row)).find()
          }

          def isDefinedAt(row: Int) =
            target.isDefinedAt(row) && pattern.isDefinedAt(row) && flag.isDefinedAt(row)
        }
    }

    // please use this one as much as possible. it is orders of magnitude faster than searchDynamic
    def search(pattern: String, flag: Boolean) = {
      val compiled = if (flag)
        Pattern.compile(pattern, Pattern.CASE_INSENSITIVE)
      else
        Pattern.compile(pattern)

      new Op1SB("search", { target =>
        compiled.matcher(target).find()
      })
    }

    // starting to follow a different pattern  since we don't do evaluator lookups anymore
    // note that this different pattern means we can't test in StringLibSpecs
    lazy val substring = CFNP {
      case List(s: StrColumn, f: LongColumn, c: LongColumn) =>
        new StrColumn {
          def apply(row: Int) = StringLib.safeSubstring(s(row), f(row).toInt, c(row).toInt)
          def isDefinedAt(row: Int) = s.isDefinedAt(row) && f.isDefinedAt(row) && c.isDefinedAt(row)
        }

      case List(s: StrColumn, f: LongColumn, c: DoubleColumn) =>
        new StrColumn {
          def apply(row: Int) = StringLib.safeSubstring(s(row), f(row).toInt, c(row).toInt)
          def isDefinedAt(row: Int) = s.isDefinedAt(row) && f.isDefinedAt(row) && c.isDefinedAt(row)
        }

      case List(s: StrColumn, f: LongColumn, c: NumColumn) =>
        new StrColumn {
          def apply(row: Int) = StringLib.safeSubstring(s(row), f(row).toInt, c(row).toInt)
          def isDefinedAt(row: Int) = s.isDefinedAt(row) && f.isDefinedAt(row) && c.isDefinedAt(row)
        }

      case List(s: StrColumn, f: DoubleColumn, c: LongColumn) =>
        new StrColumn {
          def apply(row: Int) = StringLib.safeSubstring(s(row), f(row).toInt, c(row).toInt)
          def isDefinedAt(row: Int) = s.isDefinedAt(row) && f.isDefinedAt(row) && c.isDefinedAt(row)
        }

      case List(s: StrColumn, f: DoubleColumn, c: DoubleColumn) =>
        new StrColumn {
          def apply(row: Int) = StringLib.safeSubstring(s(row), f(row).toInt, c(row).toInt)
          def isDefinedAt(row: Int) = s.isDefinedAt(row) && f.isDefinedAt(row) && c.isDefinedAt(row)
        }

      case List(s: StrColumn, f: DoubleColumn, c: NumColumn) =>
        new StrColumn {
          def apply(row: Int) = StringLib.safeSubstring(s(row), f(row).toInt, c(row).toInt)
          def isDefinedAt(row: Int) = s.isDefinedAt(row) && f.isDefinedAt(row) && c.isDefinedAt(row)
        }

      case List(s: StrColumn, f: NumColumn, c: LongColumn) =>
        new StrColumn {
          def apply(row: Int) = StringLib.safeSubstring(s(row), f(row).toInt, c(row).toInt)
          def isDefinedAt(row: Int) = s.isDefinedAt(row) && f.isDefinedAt(row) && c.isDefinedAt(row)
        }

      case List(s: StrColumn, f: NumColumn, c: DoubleColumn) =>
        new StrColumn {
          def apply(row: Int) = StringLib.safeSubstring(s(row), f(row).toInt, c(row).toInt)
          def isDefinedAt(row: Int) = s.isDefinedAt(row) && f.isDefinedAt(row) && c.isDefinedAt(row)
        }

      case List(s: StrColumn, f: NumColumn, c: NumColumn) =>
        new StrColumn {
          def apply(row: Int) = StringLib.safeSubstring(s(row), f(row).toInt, c(row).toInt)
          def isDefinedAt(row: Int) = s.isDefinedAt(row) && f.isDefinedAt(row) && c.isDefinedAt(row)
        }
    }

    object regexMatch extends Op2 with Op2Array {

      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(JTextT, JTextT, JArrayHomogeneousT(JTextT))

      lazy val prepare = CF1(Some(_))

      val mapper = CF2Array[String]("std::string::regexMatch") {
        case (target: StrColumn, regex: StrColumn, range) => {
          val table   = new Array[Array[String]](range.length)
          val defined = new BitSet(range.length)

          RangeUtil.loop(range) { i =>
            if (target.isDefinedAt(i) && regex.isDefinedAt(i)) {
              val str = target(i)

              try {
                val reg = regex(i).r

                str match {
                  case reg(capture @ _ *) => {
                    val capture2 = capture map { str =>
                      if (str == null)
                        ""
                      else
                        str
                    }

                    table(i) = capture2.toArray
                    defined.set(i)
                  }

                  case _ =>
                }
              } catch {
                case _: java.util.regex.PatternSyntaxException => // yay, scala
              }
            }
          }

          (CString, table, defined)
        }
      }
    }

    object concat extends Op2F2 {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(JTextT, JTextT, JTextT)
      def f2: F2 = CF2P {
        case (c1: StrColumn, c2: StrColumn) =>
          new StrFrom.SS(c1, c2, neitherNull, _ concat _)
      }
    }

    class Op2SLL(name: String, defined: (String, Long) => Boolean, f: (String, Long) => Long) extends Op2F2 {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(JTextT, JNumberT, JNumberT)
      def f2: F2 = CF2P {
        case (c1: StrColumn, c2: DoubleColumn) =>
          new LongFrom.SD(c1, c2, (s, n) => (n % 1 == 0) && defined(s, n.toLong), (s, n) => f(s, n.toLong))

        case (c1: StrColumn, c2: LongColumn) =>
          new LongFrom.SL(c1, c2, defined, f)

        case (c1: StrColumn, c2: NumColumn) =>
          new LongFrom.SN(c1, c2, (s, n) => (n % 1 == 0) && defined(s, n.toLong), (s, n) => f(s, n.toLong))
      }
    }

    object codePointAt extends Op2SLL("codePointAt", (s, n) => n >= 0 && s.length > n, (s, n) => s.codePointAt(n.toInt))

    object codePointBefore extends Op2SLL("codePointBefore", (s, n) => n >= 0 && s.length > n, (s, n) => s.codePointBefore(n.toInt))

    class Substring(name: String)(f: (String, Int) => String) extends Op2F2 {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(JTextT, JNumberT, JTextT)
      def f2: F2 = CF2P {
        case (c1: StrColumn, c2: LongColumn) =>
          new StrFrom.SL(c1, c2, (s, n) => n >= 0, { (s, n) =>
            f(s, n.toInt)
          })
        case (c1: StrColumn, c2: DoubleColumn) =>
          new StrFrom.SD(c1, c2, (s, n) => n >= 0 && (n % 1 == 0), { (s, n) =>
            f(s, n.toInt)
          })
        case (c1: StrColumn, c2: NumColumn) =>
          new StrFrom.SN(c1, c2, (s, n) => n >= 0 && (n % 1 == 0), { (s, n) =>
            f(s, n.toInt)
          })
      }
    }

    object takeLeft
        extends Substring("takeLeft")({ (s, n) =>
          s.substring(0, math.min(n, s.length))
        })

    object takeRight
        extends Substring("takeRight")({ (s, n) =>
          s.substring(math.max(s.length - n, 0))
        })

    object dropLeft
        extends Substring("dropLeft")({ (s, n) =>
          s.substring(math.min(n, s.length))
        })

    object dropRight
        extends Substring("dropRight")({ (s, n) =>
          s.substring(0, math.max(0, s.length - n))
        })

    class Op2SSL(name: String, f: (String, String) => Long) extends Op2F2 {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(JTextT, JTextT, JNumberT)
      def f2: F2 = CF2P {
        case (c1: StrColumn, c2: StrColumn) =>
          new LongFrom.SS(c1, c2, neitherNull, f)
      }
    }

    object compare extends Op2SSL("compare", _ compareTo _)

    object compareIgnoreCase extends Op2SSL("compareIgnoreCase", _ compareToIgnoreCase _)

    object indexOf extends Op2SSL("indexOf", _ indexOf _)

    object lastIndexOf extends Op2SSL("lastIndexOf", _ lastIndexOf _)

    object parseNum extends Op1F1 {
      val intPattern = Pattern.compile("^-?(?:0|[1-9][0-9]*)$")
      val decPattern = Pattern.compile("^-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?(?:[eE][-+]?[0-9]+)?$")

      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = UnaryOperationType(JTextT, JNumberT)

      private def build(c: StrColumn) = new Map1Column(c) with NumColumn {
        override def isDefinedAt(row: Int): Boolean = {
          if (!super.isDefinedAt(row)) false
          else {
            val s = c(row)
            s != null && decPattern.matcher(s).matches
          }
        }

        def apply(row: Int) = BigDecimal(c(row))
      }

      def f1: F1 = CF1P {
        case c: StrColumn => build(c)
      }
    }

    object numToString extends Op1F1 {
      val tpe = UnaryOperationType(JNumberT, JTextT)
      def f1: F1 = CF1P {
        case c: LongColumn => new StrFrom.L(c, _ => true, _.toString)
        case c: DoubleColumn => {
          new StrFrom.D(c, _ => true, { d =>
            val back = d.toString
            if (back.endsWith(".0"))
              back.substring(0, back.length - 2)
            else
              back
          })
        }
        case c: NumColumn => new StrFrom.N(c, _ => true, _.toString)
      }
    }

    object split extends Op2 with Op2Array {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(JTextT, JTextT, JArrayHomogeneousT(JTextT))

      lazy val prepare = CF1(Some(_))

      lazy val mapper = splitMapper(true)
    }

    object splitRegex extends Op2 with Op2Array {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(JTextT, JTextT, JArrayHomogeneousT(JTextT))

      lazy val prepare = CF1(Some(_))

      lazy val mapper = splitMapper(false)
    }

    def splitMapper(quote: Boolean) = CF2Array[String]("std::string::split(%s)".format(quote)) {
      case (left: StrColumn, right: StrColumn, range) => {
        val result  = new Array[Array[String]](range.length)
        val defined = new BitSet(range.length)

        RangeUtil.loop(range) { row =>
          if (left.isDefinedAt(row) && right.isDefinedAt(row)) {
            try {
              val pattern = if (quote) Pattern.quote(right(row)) else right(row)

              // TOOD cache compiled patterns for awesome sauce
              result(row) = Pattern.compile(pattern).split(left(row), -1)

              defined.flip(row)
            } catch {
              case _: PatternSyntaxException =>
            }
          }
        }

        (CString, result, defined)
      }
    }
  }
}
