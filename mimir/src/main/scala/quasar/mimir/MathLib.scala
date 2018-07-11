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

import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.table._

trait MathLibModule extends ColumnarTableLibModule with InfixLibModule {
  trait MathLib extends ColumnarTableLib with InfixLib {

    override def _lib1 =
      super._lib1 ++ Set(
        sinh,
        toDegrees,
        expm1,
        getExponent,
        asin,
        log10,
        cos,
        exp,
        cbrt,
        atan,
        ceil,
        rint,
        log1p,
        sqrt,
        floor,
        toRadians,
        tanh,
        round,
        cosh,
        tan,
        abs,
        sin,
        mathlog,
        signum,
        acos,
        ulp)

    override def _lib2 = super._lib2 ++ Set(minOf, hypot, pow, maxOf, atan2, copySign, roundTo, IEEEremainder)

    import StdLib.{ DoubleFrom, doubleIsDefined }
    import java.lang.Math

    object pow extends Op2F2 with Infix.Power

    abstract class Op1DD(defined: Double => Boolean, f: Double => Double) extends Op1F1 {
      val tpe = UnaryOperationType(JNumberT, JNumberT)
      def f1: F1 = CF1P {
        case c: DoubleColumn => new DoubleFrom.D(c, defined, f)
        case c: LongColumn   => new DoubleFrom.L(c, defined, f)
        case c: NumColumn    => new DoubleFrom.N(c, defined, f)
      }
    }

    object sinh extends Op1DD(doubleIsDefined, Math.sinh)

    object toDegrees extends Op1DD(doubleIsDefined, Math.toDegrees)

    object expm1 extends Op1DD(doubleIsDefined, Math.expm1)

    object getExponent extends Op1DD(n => doubleIsDefined(n) && n > 0.0, n => Math.getExponent(n).toDouble)

    object asin extends Op1DD(n => -1.0 <= n && n <= 1.0, Math.asin)

    object log10 extends Op1DD(n => doubleIsDefined(n) && n > 0.0, Math.log10)

    object cos extends Op1DD(doubleIsDefined, Math.cos)

    object exp extends Op1DD(doubleIsDefined, Math.exp)

    object cbrt extends Op1DD(doubleIsDefined, Math.cbrt)

    object atan extends Op1DD(doubleIsDefined, Math.atan)

    object ceil extends Op1DD(doubleIsDefined, Math.ceil)

    object rint extends Op1DD(doubleIsDefined, Math.rint)

    object log1p extends Op1DD(n => doubleIsDefined(n) && n > -1.0, Math.log1p)

    object sqrt extends Op1DD(n => doubleIsDefined(n) && n >= 0.0, Math.sqrt)

    object floor extends Op1DD(doubleIsDefined, Math.floor)

    object toRadians extends Op1DD(doubleIsDefined, Math.toRadians)

    object tanh extends Op1DD(doubleIsDefined, Math.tanh)

    // Math.round returns Long, so we have to improvise.
    // 4503599627370496.0 is the point where Double can't represent fractional
    // values anymore, so beyond that we just pass the value through
    object round extends Op1DD(doubleIsDefined, n => if (Math.abs(n) >= 4503599627370496.0) n else Math.round(n))

    object cosh extends Op1DD(doubleIsDefined, Math.cosh)

    object tan extends Op1DD(doubleIsDefined, Math.tan)

    object abs extends Op1DD(doubleIsDefined, Math.abs)

    object sin extends Op1DD(doubleIsDefined, Math.sin)

    object mathlog extends Op1DD(n => doubleIsDefined(n) && n > 0.0, Math.log)

    object signum extends Op1DD(doubleIsDefined, Math.signum)

    object acos extends Op1DD(n => doubleIsDefined(n) && -1.0 <= n && n <= 1.0, Math.acos)

    object ulp extends Op1DD(doubleIsDefined, Math.ulp)

    abstract class Op2DDD(defined: (Double, Double) => Boolean, f: (Double, Double) => Double) extends Op2F2 {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
      def f2: F2 = CF2P {
        case (c1: DoubleColumn, c2: DoubleColumn) =>
          new DoubleFrom.DD(c1, c2, defined, f)

        case (c1: DoubleColumn, c2: LongColumn) =>
          new DoubleFrom.DL(c1, c2, defined, f)

        case (c1: DoubleColumn, c2: NumColumn) =>
          new DoubleFrom.DN(c1, c2, defined, f)

        case (c1: LongColumn, c2: DoubleColumn) =>
          new DoubleFrom.LD(c1, c2, defined, f)

        case (c1: NumColumn, c2: DoubleColumn) =>
          new DoubleFrom.ND(c1, c2, defined, f)

        case (c1: LongColumn, c2: LongColumn) =>
          new DoubleFrom.LL(c1, c2, defined, f)

        case (c1: LongColumn, c2: NumColumn) =>
          new DoubleFrom.LN(c1, c2, defined, f)

        case (c1: NumColumn, c2: LongColumn) =>
          new DoubleFrom.NL(c1, c2, defined, f)

        case (c1: NumColumn, c2: NumColumn) =>
          new DoubleFrom.NN(c1, c2, defined, f)
      }
    }

    def bothDefined(x: Double, y: Double) = doubleIsDefined(x) && doubleIsDefined(y)

    object minOf extends Op2DDD(bothDefined, Math.min)

    object hypot extends Op2DDD(bothDefined, Math.hypot)

    object maxOf extends Op2DDD(bothDefined, Math.max)

    object atan2 extends Op2DDD(bothDefined, Math.atan2)

    object copySign extends Op2DDD(bothDefined, Math.copySign)

    object IEEEremainder extends Op2DDD(bothDefined, Math.IEEEremainder)

    object roundTo
        extends Op2DDD(bothDefined, { (n, digits) =>
          val adjusted = n * math.pow(10, digits)
          val rounded  = if (Math.abs(n) >= 4503599627370496.0) adjusted else Math.round(adjusted)

          rounded / math.pow(10, digits)
        })
  }
}
