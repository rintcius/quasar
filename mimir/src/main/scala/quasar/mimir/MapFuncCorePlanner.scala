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

import quasar.common.data.Data
import quasar.contrib.iota.copkTraverse
import quasar.contrib.std.{errorImpossible, errorNotImplemented}
import quasar.precog.common._
import quasar.qscript.{MapFuncCore, MapFuncsCore}
import quasar.yggdrasil.bytecode._

import matryoshka.{AlgebraM, RecursiveT}
import matryoshka.implicits._

import scalaz.Applicative
import scalaz.syntax.applicative._

final class MapFuncCorePlanner[T[_[_]]: RecursiveT, F[_]: Applicative]
  extends MapFuncPlanner[T, F, MapFuncCore[T, ?]] {

  def plan(cake: Precog): PlanApplicator[cake.type] =
    new PlanApplicatorCore(cake)

  final class PlanApplicatorCore[P <: Precog](override val cake: P)
    extends PlanApplicator[P](cake) {

    import cake.trans._
    import cake.Library._

    private def undefined[A <: SourceType](id: TransSpec[A]): TransSpec[A] =
      DerefArrayStatic[A](OuterArrayConcat[A](WrapArray(id)), CPathIndex(1))

    def apply[A <: SourceType](id: TransSpec[A]): AlgebraM[F, MapFuncCore[T, ?], TransSpec[A]] = {
      case MapFuncsCore.Undefined() =>
        undefined(id).point[F]

      case MapFuncsCore.Constant(ejson) =>
        // EJson => Data => RValue => Table
        val data: Data = ejson.cata(Data.fromEJson)
        val rvalue = RValue.fromData(data)
        rvalue.map(transRValue(_, id)).getOrElse(undefined(id)).point[F]

      case MapFuncsCore.JoinSideName(_) => errorImpossible // should never be received

      case MapFuncsCore.ArrayLength(a1) =>
        (ArrayLength[A](a1): TransSpec[A]).point[F]

      case MapFuncsCore.Length(a1) => length.spec(a1).point[F]

      case MapFuncsCore.ExtractCentury(a1) =>
        (Map1[A](a1, cake.Library.ExtractCentury.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractDayOfMonth(a1) =>
        (Map1[A](a1, cake.Library.ExtractDayOfMonth.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractDecade(a1) =>
        (Map1[A](a1, cake.Library.ExtractDecade.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractDayOfWeek(a1) =>
        (Map1[A](a1, cake.Library.ExtractDayOfWeek.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractDayOfYear(a1) =>
        (Map1[A](a1, cake.Library.ExtractDayOfYear.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractEpoch(a1) =>
        (Map1[A](a1, cake.Library.ExtractEpoch.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractHour(a1) =>
        (Map1[A](a1, cake.Library.ExtractHour.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractIsoDayOfWeek(a1) =>
        (Map1[A](a1, cake.Library.ExtractIsoDayOfWeek.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractIsoYear(a1) =>
        (Map1[A](a1, cake.Library.ExtractIsoYear.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractMicrosecond(a1) =>
        (Map1[A](a1, cake.Library.ExtractMicrosecond.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractMillennium(a1) =>
        (Map1[A](a1, cake.Library.ExtractMillennium.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractMillisecond(a1) =>
        (Map1[A](a1, cake.Library.ExtractMillisecond.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractMinute(a1) =>
        (Map1[A](a1, cake.Library.ExtractMinute.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractMonth(a1) =>
        (Map1[A](a1, cake.Library.ExtractMonth.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractQuarter(a1) =>
        (Map1[A](a1, cake.Library.ExtractQuarter.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractSecond(a1) =>
        (Map1[A](a1, cake.Library.ExtractSecond.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractTimeZone(a1) =>
        (Map1[A](a1, cake.Library.ExtractTimeZone.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractTimeZoneHour(a1) =>
        (Map1[A](a1, cake.Library.ExtractTimeZoneHour.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractTimeZoneMinute(a1) =>
        (Map1[A](a1, cake.Library.ExtractTimeZoneMinute.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractWeek(a1) =>
        (Map1[A](a1, cake.Library.ExtractWeek.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractYear(a1) =>
        (Map1[A](a1, cake.Library.ExtractYear.f1): TransSpec[A]).point[F]
      case MapFuncsCore.LocalDateTime(a1) =>
        (Map1[A](a1, cake.Library.LocalDateTime.f1): TransSpec[A]).point[F]
      case MapFuncsCore.LocalDate(a1) =>
        (Map1[A](a1, cake.Library.LocalDate.f1): TransSpec[A]).point[F]
      case MapFuncsCore.LocalTime(a1) =>
        (Map1[A](a1, cake.Library.LocalTime.f1): TransSpec[A]).point[F]
      case MapFuncsCore.OffsetDateTime(a1) =>
        (Map1[A](a1, cake.Library.OffsetDateTime.f1): TransSpec[A]).point[F]
      case MapFuncsCore.OffsetTime(a1) =>
        (Map1[A](a1, cake.Library.OffsetTime.f1): TransSpec[A]).point[F]
      case MapFuncsCore.OffsetDate(a1) =>
        (Map1[A](a1, cake.Library.OffsetDate.f1): TransSpec[A]).point[F]
      case MapFuncsCore.Interval(a1) =>
        (Map1[A](a1, cake.Library.Interval.f1): TransSpec[A]).point[F]
      case MapFuncsCore.StartOfDay(a1) =>
        (Map1[A](a1, cake.Library.StartOfDay.f1): TransSpec[A]).point[F]
      case MapFuncsCore.TemporalTrunc(part, a1) =>
        (Map1[A](a1, cake.Library.truncPart(part).f1): TransSpec[A]).point[F]
      case MapFuncsCore.TimeOfDay(a1) =>
        (Map1[A](a1, cake.Library.TimeOfDay.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ToTimestamp(a1) =>
        (Map1[A](a1, cake.Library.ToTimestamp.f1): TransSpec[A]).point[F]

      case MapFuncsCore.Now() => errorNotImplemented
      case MapFuncsCore.NowTime() => errorNotImplemented
      case MapFuncsCore.NowDate() => errorNotImplemented
      case MapFuncsCore.CurrentTimeZone() => errorNotImplemented

      case MapFuncsCore.SetTimeZone(v, a1) =>
        (Map2[A](a1, v, cake.Library.SetTimeZone.f2): TransSpec[A]).point[F]
      case MapFuncsCore.SetTimeZoneHour(v, a1) =>
        (Map2[A](a1, v, cake.Library.SetTimeZoneHour.f2): TransSpec[A]).point[F]
      case MapFuncsCore.SetTimeZoneMinute(v, a1) =>
        (Map2[A](a1, v, cake.Library.SetTimeZoneMinute.f2): TransSpec[A]).point[F]

      case MapFuncsCore.TypeOf(a1) =>
        (Map1[A](a1, cake.Library.typeOf.f1): TransSpec[A]).point[F]

      case MapFuncsCore.Negate(a1) =>
        Unary.Neg.spec(a1).point[F]   // NB: don't use math.Negate here; it does weird things to booleans
      case MapFuncsCore.Add(a1, a2) =>
        Infix.Add.spec(a1, a2).point[F]
      case MapFuncsCore.Multiply(a1, a2) =>
        Infix.Mul.spec(a1, a2).point[F]
      case MapFuncsCore.Subtract(a1, a2) =>
        Infix.Sub.spec(a1, a2).point[F]
      case MapFuncsCore.Divide(a1, a2) =>
        Infix.Div.spec(a1, a2).point[F]
      case MapFuncsCore.Modulo(a1, a2) =>
        Infix.Mod.spec(a1, a2).point[F]
      case MapFuncsCore.Power(a1, a2) =>
        Infix.Pow.spec(a1, a2).point[F]

      case MapFuncsCore.Not(a1) =>
        Unary.Comp.spec(a1).point[F]

      case MapFuncsCore.Eq(a1, ConstLiteral(literal, _)) =>
        (EqualLiteral[A](a1, literal, false): TransSpec[A]).point[F]

      case MapFuncsCore.Eq(ConstLiteral(literal, _), a2) =>
        (EqualLiteral[A](a2, literal, false): TransSpec[A]).point[F]

      case MapFuncsCore.Eq(a1, a2) =>
        (Equal[A](a1, a2): TransSpec[A]).point[F]

      case MapFuncsCore.Neq(a1, ConstLiteral(literal, _)) =>
        (EqualLiteral[A](a1, literal, true): TransSpec[A]).point[F]

      case MapFuncsCore.Neq(ConstLiteral(literal, _), a2) =>
        (EqualLiteral[A](a2, literal, true): TransSpec[A]).point[F]

      case MapFuncsCore.Neq(a1, a2) =>
        Unary.Comp.spec(Equal[A](a1, a2)).point[F]

      case MapFuncsCore.Lt(a1, a2) =>
        Infix.Lt.spec(a1, a2).point[F]
      case MapFuncsCore.Lte(a1, a2) =>
        Infix.LtEq.spec(a1, a2).point[F]
      case MapFuncsCore.Gt(a1, a2) =>
        Infix.Gt.spec(a1, a2).point[F]
      case MapFuncsCore.Gte(a1, a2) =>
        Infix.GtEq.spec(a1, a2).point[F]

      case MapFuncsCore.IfUndefined(a1, a2) =>
        (IfUndefined(a1, a2): TransSpec[A]).point[F]

      case MapFuncsCore.And(a1, a2) =>
        Infix.And.spec2(a1, a2).point[F]
      case MapFuncsCore.Or(a1, a2) =>
        Infix.Or.spec2(a1, a2).point[F]
      case MapFuncsCore.Between(a1, a2, a3) =>
        (MapN(OuterArrayConcat(WrapArray(a1), WrapArray(a2), WrapArray(a3)), between): TransSpec[A]).point[F]
      case MapFuncsCore.Cond(a1, a2, a3) if a3 == undefined(id) =>
        (Filter(a2, a1): TransSpec[A]).point[F]
      case MapFuncsCore.Cond(a1, a2, a3) =>
        (Cond(a1, a2, a3): TransSpec[A]).point[F]

      case MapFuncsCore.Within(a1, a2) =>
        (Within(a1, a2): TransSpec[A]).point[F]

      case MapFuncsCore.Lower(a1) =>
        toLowerCase.spec(a1).point[F]
      case MapFuncsCore.Upper(a1) =>
        toUpperCase.spec(a1).point[F]
      case MapFuncsCore.Bool(a1) =>
        readBoolean.spec[A](a1).point[F]
      case MapFuncsCore.Integer(a1) =>
        readInteger.spec[A](a1).point[F]
      case MapFuncsCore.Decimal(a1) =>
        readDecimal.spec[A](a1).point[F]
      case MapFuncsCore.Number(a1) =>
        (ToNumber(a1): TransSpec[A]).point[F]
      case MapFuncsCore.Null(a1) =>
        readNull.spec[A](a1).point[F]
      case MapFuncsCore.ToString(a1) =>
        convertToString.spec[A](a1).point[F]
      case MapFuncsCore.Split(a1, a2) =>
        split.spec[A](a1, a2).point[F]

      // significantly faster fast path
      case MapFuncsCore.Search(src, ConstLiteral(CString(pattern), _), ConstLiteral(CBoolean(flag), _)) =>
        search(pattern, flag).spec[A](src).point[F]

      // this case is hideously slow; hopefully we don't see it too often
      case MapFuncsCore.Search(src, pattern, flag) =>
        (MapN((OuterArrayConcat(WrapArray(src), WrapArray(pattern), WrapArray(flag))), searchDynamic): TransSpec[A]).point[F]

      // significantly faster fast path
      case MapFuncsCore.Like(src, ConstLiteral(CString(pattern), _), ConstLiteral(CString(escape), _)) if escape.length <= 1 =>
        like(pattern, escape.headOption).spec[A](src).point[F]

      // this case is hideously slow; hopefully we don't see it too often
      case MapFuncsCore.Like(src, pattern, escape) =>
        (MapN((OuterArrayConcat(WrapArray(src), WrapArray(pattern), WrapArray(escape))), likeDynamic): TransSpec[A]).point[F]

      case MapFuncsCore.Substring(string, from, count) =>
        val args = OuterArrayConcat(WrapArray(string), WrapArray(from), WrapArray(count))
        (MapN(args, substring): TransSpec[A]).point[F]

      case MapFuncsCore.MakeArray(a1) =>
        (WrapArray[A](a1): TransSpec[A]).point[F]
      case MapFuncsCore.MakeMap(ConstLiteral(CString(key), _), value) =>
        (WrapObject[A](value, key): TransSpec[A]).point[F]
      case MapFuncsCore.MakeMap(key, value) =>
        (WrapObjectDynamic[A](key, value): TransSpec[A]).point[F]

      case MapFuncsCore.ConcatArrays(a1, a2) =>
        (Cond(
          Infix.And.spec2[A](IsType(a1, JTextT), IsType(a2, JTextT)),
          concat.spec[A](a1, a2),
          OuterArrayConcat[A](a1, a2)): TransSpec[A]).point[F]

      case MapFuncsCore.ConcatMaps(a1, a2) =>
        (OuterObjectConcat[A](a1, a2): TransSpec[A]).point[F]

      case MapFuncsCore.ProjectIndex(src, ConstLiteral(CLong(index), _)) =>
        (DerefArrayStatic[A](src, CPathIndex(index.toInt)): TransSpec[A]).point[F]
      case MapFuncsCore.ProjectIndex(src, index) =>
        (DerefArrayDynamic[A](src, index): TransSpec[A]).point[F]

      case MapFuncsCore.ProjectKey(src, ConstLiteral(CString(field), _)) =>
        (DerefObjectStatic[A](src, CPathField(field)): TransSpec[A]).point[F]
      case MapFuncsCore.ProjectKey(src, field) =>
        (DerefObjectDynamic[A](src, field): TransSpec[A]).point[F]

      case MapFuncsCore.DeleteKey(src, ConstLiteral(CString(key), _)) =>
        (ObjectDelete[A](src, Set(CPathField(key))): TransSpec[A]).point[F]

      // mimir doesn't have a way to implement this
      case MapFuncsCore.DeleteKey(src, field) => errorNotImplemented

      case MapFuncsCore.ContainsKey(src, ConstLiteral(CString(key), _)) =>
        (IsType[A](
          Typed[A](src, JObjectUnfixedT),
          JObjectFixedT(Map(key -> JType.JUniverseT))): TransSpec[A]).point[F]

      // mimir doesn't have a way to implement this
      case MapFuncsCore.ContainsKey(_, _) => errorNotImplemented

      case MapFuncsCore.Meta(a1) => errorNotImplemented

      case MapFuncsCore.Range(from, to) =>
        (Range(from, to): TransSpec[A]).point[F]

      case MapFuncsCore.Guard(src, tpe, a2, a3) =>
        (Cond(IsType(src, JType.fromType(tpe)), a2, a3): TransSpec[A]).point[F]
    }
  }
}
