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

package quasar.qsu

import slamdata.Predef._
import quasar.{Qspec, TreeMatchers, Type}
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript.{
  construction,
  ExcludeId,
  IdOnly,
  IncludeId,
  Hole,
  LeftSide,
  MapFuncsCore,
  OnUndefined,
  PlannerError,
  ReduceFuncs,
  ReduceIndex,
  RightSide,
  SrcHole
}
import quasar.qscript.provenance.Dimensions
import matryoshka._
import matryoshka.data.Fix
import matryoshka.data.free._
import pathy.Path
import Path.Sandboxed

import scalaz.{\/, \/-, EitherT, Free, Need, StateT}
import scalaz.std.anyVal._
import scalaz.syntax.applicative._
import scalaz.syntax.either._
import scalaz.syntax.tag._
import scalaz.syntax.std.boolean._

object MinimizeAutoJoinsSpec extends Qspec with TreeMatchers with QSUTTypes[Fix] {
  import QSUGraph.Extractors._
  import ApplyProvenance.AuthenticatedQSU
  import QScriptUniform.{DTrans, Retain, Rotation}

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  val qsu = QScriptUniform.DslT[Fix]
  val func = construction.Func[Fix]
  val recFunc = construction.RecFunc[Fix]
  val qprov = QProv[Fix]

  type J = Fix[EJson]
  val J = Fixed[Fix[EJson]]

  val afile = Path.rootDir[Sandboxed] </> Path.file("afile")
  val afile2 = Path.rootDir[Sandboxed] </> Path.file("afile2")

  val shiftedRead =
    qsu.leftShift(qsu.read(afile), recFunc.Hole, ExcludeId, OnUndefined.Omit, RightTarget[Fix], Rotation.ShiftMap)

  import qprov.prov.implicits._

  "autojoin minimization" should {
    "linearize .foo + .bar" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.map(
            qsu.read(afile),
            recFunc.ProjectKeyS(recFunc.Hole, "foo")),
          qsu.map(
            qsu.read(afile),
            recFunc.ProjectKeyS(recFunc.Hole, "bar")),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(Read(_), fm) =>
          // must_=== doesn't work
          fm must beTreeEqual(
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "foo"),
              recFunc.ProjectKeyS(recFunc.Hole, "bar")))
      }
    }

    "convert Typecheck to a Map(_, Guard)" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin3((
          qsu.read(afile),
          qsu.read(afile),
          qsu.undefined(),
          _(MapFuncsCore.Guard(_, Type.AnyObject, _, _)))))

      runOn(qgraph) must beLike {
        case Map(Read(_), fm) =>
          // must_=== doesn't work
          fm must beTreeEqual(recFunc.Guard(recFunc.Hole, Type.AnyObject, recFunc.Hole, recFunc.Undefined))
      }
    }

    "coalesce two summed reductions" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Count(func.Hole)),
            func.ReduceIndex(\/-(0))),
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Sum(func.Hole)),
            func.ReduceIndex(\/-(0))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            Read(_),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repair),
          fm) =>

          // must_=== doesn't work
          h1 must beTreeEqual(func.Hole)
          h2 must beTreeEqual(func.Hole)

          repair must beTreeEqual(
            func.StaticMapS(
              "0" ->
                func.ReduceIndex(\/-(0)),
              "1" ->
                func.ReduceIndex(\/-(1))))

          fm must beTreeEqual(
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "0"),
              recFunc.ProjectKeyS(recFunc.Hole, "1")))
      }
    }

    "coalesce three summed reductions" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.autojoin2((
            qsu.qsReduce(
              qsu.read(afile),
              Nil,
              List(ReduceFuncs.Count(func.Hole)),
              func.ReduceIndex(\/-(0))),
            qsu.qsReduce(
              qsu.read(afile),
              Nil,
              List(ReduceFuncs.Sum(func.Hole)),
              func.ReduceIndex(\/-(0))),
            _(MapFuncsCore.Add(_, _)))),
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Max(func.Hole)),
            func.ReduceIndex(\/-(0))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            Read(_),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2), ReduceFuncs.Max(h3)),
            repair),
          fm) =>

          // must_=== doesn't work
          h1 must beTreeEqual(func.Hole)
          h2 must beTreeEqual(func.Hole)
          h3 must beTreeEqual(func.Hole)

          repair must beTreeEqual(
            func.StaticMapS(
              "0" -> func.ReduceIndex(\/-(0)),
              "1" -> func.ReduceIndex(\/-(1)),
              "2" -> func.ReduceIndex(\/-(2))))

          fm must beTreeEqual(
            recFunc.Add(
              recFunc.Add(
                recFunc.ProjectKeyS(recFunc.Hole, "0"),
                recFunc.ProjectKeyS(recFunc.Hole, "1")),
              recFunc.ProjectKeyS(recFunc.Hole, "2")))
      }
    }

    "coalesce two summed reductions, one downstream of an autojoin" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.autojoin2((
              qsu.read(afile),
              qsu.map1((
                qsu.unreferenced(),
                MapFuncsCore.Constant[Fix, Hole](J.str("hey")))),
              _(MapFuncsCore.ConcatArrays(_, _)))),
            Nil,
            List(ReduceFuncs.Count(func.Hole)),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Sum(func.Hole)),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            Read(_),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repair),
          fm) =>

          // must_=== doesn't work
          h1 must beTreeEqual(
            func.ConcatArrays(
              func.Hole,
              func.Constant(J.str("hey"))))

          h2 must beTreeEqual(func.Hole)

          repair must beTreeEqual(
            func.StaticMapS(
              "0" ->
                func.ReduceIndex(\/-(0)),
              "1" ->
                func.ReduceIndex(\/-(1))))

          fm must beTreeEqual(
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "0"),
              recFunc.ProjectKeyS(recFunc.Hole, "1")))
      }
    }

    "rewrite filter into cond only to avoid join" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.read(afile),
          qsu.qsFilter(
            qsu.read(afile),
            recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo")))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(Read(_), fm) =>
          fm must beTreeEqual(
            recFunc.Add(
              recFunc.Hole,
              recFunc.Cond(
                recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo"))),
                recFunc.Hole,
                recFunc.Undefined)))
      }
    }

    "rewrite filter into cond when there are two filters" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsFilter(
            qsu.read(afile),
            recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo")))),
          qsu.qsFilter(
            qsu.read(afile),
            recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("bar")))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(Read(_), fm) =>
          fm must beTreeEqual(
            recFunc.Add(
              recFunc.Cond(
                recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo"))),
                recFunc.Hole,
                recFunc.Undefined),
              recFunc.Cond(
                recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("bar"))),
                recFunc.Hole,
                recFunc.Undefined)))
      }
    }

    "rewrite filter into cond to avoid randomly creating more shifts" in {
      val shift = qsu.leftShift(
        shiftedRead,
        recFunc.Hole,
        ExcludeId,
        OnUndefined.Omit,
        RightTarget[Fix],
        Rotation.ShiftArray)

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          shift,
          qsu.qsFilter(
            shift,
            recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo")))),
          _(MapFuncsCore.Add(_, _)))))

      val results = runOn(qgraph)

      results must beLike {
        case
          Map(
            LeftShift(
              LeftShift(Read(`afile`), _, _, _, _, _),
              _, _, _, _, _),
            fm) =>

          fm must beTreeEqual(
            recFunc.Add(
              recFunc.Hole,
              recFunc.Cond(
                recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo"))),
                recFunc.Hole,
                recFunc.Undefined)))
      }
    }

    "not rewrite filter acting as upstream source" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsFilter(
            qsu.read(afile),
            recFunc.Eq(recFunc.Hole, recFunc.Constant(J.str("foo")))),
          qsu.cint(42),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(QSFilter(_, _), fm) =>
          fm must beTreeEqual(
            recFunc.Add(recFunc.Hole, recFunc.Constant(J.int(42))))
      }
    }

    "not unnecessarily rewrite filter" in {
      val tread =
        qsu.tread(afile)

      val filter =
        qsu.qsFilter(
          qsu.autojoin3((
            tread,
            tread,
            qsu.undefined(),
            _(MapFuncsCore.Guard(_, Type.AnyObject, _, _)))),
          recFunc.Eq(
            recFunc.ProjectKeyS(recFunc.Hole, "city"),
            recFunc.ProjectKeyS(recFunc.Hole, "state")))

      val projLoc =
        qsu.autojoin2((
          filter,
          qsu.cstr("loc"),
          _(MapFuncsCore.ProjectKey(_, _))))

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.autojoin2((
            qsu.cstr("city"),
            qsu.autojoin2((
              filter,
              qsu.cstr("city"),
              _(MapFuncsCore.ProjectKey(_, _)))),
            _(MapFuncsCore.MakeMap(_, _)))),
          qsu.autojoin2((
            qsu.cstr("loc"),
            qsu.transpose(
              qsu.autojoin3((
                projLoc,
                projLoc,
                qsu.undefined(),
                _(MapFuncsCore.Guard(_, Type.AnyArray, _, _)))),
              Retain.Values,
              Rotation.FlattenArray),
            _(MapFuncsCore.MakeMap(_, _)))),
        _(MapFuncsCore.ConcatMaps(_, _)))))

      runOn(qgraph).foldMapUp {
        case QSFilter(_, _) => true.disjunction
        case _              => false.disjunction
      }.unwrap must beTrue
    }.pendingUntilFixed

    "coalesce two summed bucketing reductions, inlining functions into the buckets" in {
      val readAndThings =
        qsu.map1((
          qsu.read(afile),
          MapFuncsCore.Negate(SrcHole)))

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            readAndThings,
            List(AccessHole[Fix]),
            List(ReduceFuncs.Count(func.Hole)),
            func.ReduceIndex(\/-(0))),
          qsu.qsReduce(
            readAndThings,
            List(AccessHole[Fix]),
            List(ReduceFuncs.Sum(func.Hole)),
            func.ReduceIndex(\/-(0))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            Read(_),
            List(bucket),
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repair),
          fm) =>

          // must_=== doesn't work
          bucket must beTreeEqual(func.Negate(AccessHole[Fix]))

          h1 must beTreeEqual(func.Negate(func.Hole))
          h2 must beTreeEqual(func.Negate(func.Hole))

          repair must beTreeEqual(
            func.StaticMapS(
              "0" ->
                func.ReduceIndex(\/-(0)),
              "1" ->
                func.ReduceIndex(\/-(1))))

          fm must beTreeEqual(
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "0"),
              recFunc.ProjectKeyS(recFunc.Hole, "1")))
      }
    }

    "remap coalesced bucket references in dimensions" in {
      val readAndThings =
        qsu.map((
          qsu.dimEdit((
            qsu.read(afile),
            DTrans.Group(func.ProjectKeyS(func.Hole, "label")))),
          recFunc.Negate(recFunc.ProjectKeyS(recFunc.Hole, "metric"))))

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2(((
          qsu.lpReduce((
            readAndThings,
            ReduceFuncs.Count(()))),
          qsu.lpReduce((
            readAndThings,
            ReduceFuncs.Sum(()))),
          _(MapFuncsCore.Add(_, _))))))

      val AuthenticatedQSU(agraph, auth) = runOn_(qgraph)

      agraph must beLike {
        case m @ Map(r @ QSReduce(_, _, _, _), _) =>
          val expDims =
            Dimensions.origin(
              qprov.prov.thenn(
                qprov.prov.both(
                  qprov.prov.injValue(J.str("0")),
                  qprov.prov.injValue(J.str("1"))),
                qprov.prov.value(IdAccess.bucket(r.root, 0))))

          auth.dims(m.root) must_= expDims
      }
    }

    "leave uncoalesced reductions of different bucketing" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Sum(func.Hole)),
            func.ReduceIndex(\/-(0))),
          qsu.qsReduce(
            qsu.read(afile),
            List(func.ProjectKeyS(AccessHole[Fix], "state")),
            List(ReduceFuncs.Sum(func.Hole)),
            func.ReduceIndex(\/-(0))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case AutoJoin2C(
          QSReduce(Read(_), Nil, _, _),
          QSReduce(Read(_), List(_), _, _),
          MapFuncsCore.Add(LeftSide, RightSide)) => ok
      }
    }

    "minimize an autojoin after prior source failure" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.autojoin2((
            qsu.read(afile),
            qsu.read(afile2),
            _(MapFuncsCore.Subtract(_, _)))),
          qsu.cint(42),
          _(MapFuncsCore.Add(_, _)))))

      val results = runOn(qgraph)

      results must beLike {
        case AutoJoin2(Read(`afile`), Read(`afile2`), fm) =>
          fm must beTreeEqual(
            func.Add(
              func.Subtract(
                func.LeftSide,
                func.RightSide),
              func.Constant(J.int(42))))
      }
    }

    "halt minimization at a grouped vertex" in {
      val groupKey =
        func.Lower(func.ProjectKeyS(func.Hole, "city"))

      val groupedGuardedRead =
        qsu.dimEdit(
          qsu.autojoin3((
            qsu.read(afile),
            qsu.read(afile),
            qsu.undefined(),
            _(MapFuncsCore.Guard(_, Type.AnyObject, _, _)))),
          DTrans.Group(groupKey))

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.autojoin2((
            qsu.cstr("city"),
            qsu.autojoin2((
              groupedGuardedRead,
              qsu.cstr("city"),
              _(MapFuncsCore.ProjectKey(_, _)))),
            _(MapFuncsCore.MakeMap(_, _)))),
          qsu.autojoin2((
            qsu.cstr("1"),
            qsu.lpReduce(
              qsu.autojoin2((
                groupedGuardedRead,
                qsu.cstr("pop"),
                _(MapFuncsCore.ProjectKey(_, _)))),
              ReduceFuncs.Sum(())),
            _(MapFuncsCore.MakeMap(_, _)))),
          _(MapFuncsCore.ConcatMaps(_, _)))))

      runOn(qgraph) must beLike {
        case AutoJoin2(
          Map(Read(_), guardL),
          QSReduce(
            Map(Read(_), guardR),
            bucket :: Nil,
            ReduceFuncs.Sum(prjPop) :: Nil,
            _),
          repair) =>

          guardL must beTreeEqual(guardR)

          repair must beTreeEqual(
            func.ConcatMaps(
              func.MakeMapS("city", func.ProjectKeyS(func.LeftSide, "city")),
              func.MakeMapS("1", func.RightSide)))

          prjPop must beTreeEqual(func.ProjectKeyS(func.Hole, "pop"))
      }
    }

    "coalesce an autojoin on a single leftshift on a shared source" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.leftShift(
            shiftedRead,
            recFunc.Hole,
            ExcludeId,
            OnUndefined.Omit,
            RightTarget[Fix],
            Rotation.ShiftArray),
          shiftedRead,
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(Read(`afile`), _, _, _, _, _),
              struct,
              ExcludeId,
              _,
              repair,
              _),
            fm) =>

          struct must beTreeEqual(recFunc.Hole)

          repair must beTreeEqual(
            func.StaticMapS(
              "0" -> RightTarget[Fix],
              "1" -> AccessLeftTarget[Fix](Access.value(_))))

          fm must beTreeEqual(
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "0"),
              recFunc.ProjectKeyS(recFunc.Hole, "1")))
      }
    }

    "coalesce an autojoin on a single leftshift on a shared source (RTL)" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          shiftedRead,
          qsu.leftShift(
            shiftedRead,
            recFunc.Hole,
            ExcludeId,
            OnUndefined.Omit,
            RightTarget[Fix],
            Rotation.ShiftArray),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(Read(`afile`), _, _, _, _, _),
              struct,
              ExcludeId,
              OnUndefined.Emit,
              repair,
              _),
            fm) =>

          struct must beTreeEqual(recFunc.Hole)

          repair must beTreeEqual(
            func.StaticMapS(
              "1" -> RightTarget[Fix],
              "0" -> AccessLeftTarget[Fix](Access.value(_))))

          fm must beTreeEqual(
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "0"),
              recFunc.ProjectKeyS(recFunc.Hole, "1")))
      }
    }

    "inductively coalesce reduces on coalesced shifts" in {
      // count(a[*]) + sum(a)
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.leftShift(
              shiftedRead,
              recFunc.Hole,
              ExcludeId,
              OnUndefined.Omit,
              RightTarget[Fix],
              Rotation.ShiftArray),
            Nil,
            List(ReduceFuncs.Count(func.Hole)),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          qsu.qsReduce(
            shiftedRead,
            Nil,
            List(ReduceFuncs.Sum(func.Hole)),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            LeftShift(
              LeftShift(Read(`afile`), _, _, _, _, _),
              struct,
              ExcludeId,
              OnUndefined.Emit,
              repairInner,
              _),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repairOuter),
          fm) =>

        struct must beTreeEqual(recFunc.Hole)

        repairInner must beTreeEqual(
          func.StaticMapS(
            "0" -> RightTarget[Fix],
            "1" -> AccessLeftTarget[Fix](Access.value(_))))

        h1 must beTreeEqual(func.ProjectKeyS(func.Hole, "0"))
        h2 must beTreeEqual(func.ProjectKeyS(func.Hole, "1"))

        repairOuter must beTreeEqual(
          func.StaticMapS(
            "0" -> func.ReduceIndex(\/-(0)),
            "1" -> func.ReduceIndex(\/-(1))))

        fm must beTreeEqual(
          recFunc.Add(
            recFunc.ProjectKeyS(recFunc.Hole, "0"),
            recFunc.ProjectKeyS(recFunc.Hole, "1")))
      }
    }

    "inductively coalesce reduces on coalesced shifts" in {
      // count(a[*]) + sum(a)
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.leftShift(
              shiftedRead,
              recFunc.Hole,
              ExcludeId,
              OnUndefined.Omit,
              RightTarget[Fix],
              Rotation.ShiftArray),
            Nil,
            List(ReduceFuncs.Count(func.Hole)),
            func.ReduceIndex(\/-(0))),
          qsu.qsReduce(
            shiftedRead,
            Nil,
            List(ReduceFuncs.Sum(func.Hole)),
            func.ReduceIndex(\/-(0))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            LeftShift(
              LeftShift(Read(`afile`), _, _, _, _, _),
              struct,
              ExcludeId,
              OnUndefined.Emit,
              repairInner,
              _),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repairOuter),
          fm) =>

        struct must beTreeEqual(recFunc.Hole)

        repairInner must beTreeEqual(
          func.StaticMapS(
            "0" -> RightTarget[Fix],
            "1" -> AccessLeftTarget[Fix](Access.value(_))))

        h1 must beTreeEqual(func.ProjectKeyS(func.Hole, "0"))
        h2 must beTreeEqual(func.ProjectKeyS(func.Hole, "1"))

        repairOuter must beTreeEqual(
          func.StaticMapS(
            "0" -> func.ReduceIndex(\/-(0)),
            "1" -> func.ReduceIndex(\/-(1))))

        fm must beTreeEqual(
          recFunc.Add(
            recFunc.ProjectKeyS(recFunc.Hole, "0"),
            recFunc.ProjectKeyS(recFunc.Hole, "1")))
      }
    }

    "coalesce an autojoin on two leftshifts on a shared source" in {
      // a[*][*] + a
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.leftShift(
            qsu.leftShift(
              shiftedRead,
              recFunc.Hole,
              IncludeId,
              OnUndefined.Omit,
              RightTarget[Fix],
              Rotation.ShiftArray),
            recFunc.Hole,
            ExcludeId,
            OnUndefined.Omit,
            RightTarget[Fix],
            Rotation.ShiftArray),
          shiftedRead,
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(
                LeftShift(Read(`afile`), _, _, _, _, _),
                structInner,
                IncludeId,
                OnUndefined.Emit,
                repairInner,
                _),
              structOuter,
              ExcludeId,
              OnUndefined.Emit,
              repairOuter,
              _),
            fm) =>

          structInner must beTreeEqual(recFunc.Hole)

          repairInner must beTreeEqual(
            func.StaticMapS(
              "original" -> AccessLeftTarget[Fix](Access.value(_)),
              "results" -> RightTarget[Fix]))

          structOuter must beTreeEqual(
            recFunc.ProjectKeyS(recFunc.Hole, "results"))

          repairOuter must beTreeEqual(
            func.StaticMapS(
              "0" ->
                RightTarget[Fix],
              "1" ->
                func.ProjectKeyS(
                  AccessLeftTarget[Fix](Access.value(_)),
                  "original")))

          fm must beTreeEqual(
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "0"),
              recFunc.ProjectKeyS(recFunc.Hole, "1")))
      }
    }

    "coalesce an autojoin on three leftshifts on a shared source" in {
      // a[*][*][*] + a
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.leftShift(
            qsu.leftShift(
              qsu.leftShift(
                shiftedRead,
                recFunc.Hole,
                IncludeId,
                OnUndefined.Omit,
                RightTarget[Fix],
                Rotation.ShiftArray),
              recFunc.Hole,
              ExcludeId,
              OnUndefined.Omit,
              RightTarget[Fix],
              Rotation.ShiftMap),
            recFunc.Hole,
            ExcludeId,
            OnUndefined.Omit,
            RightTarget[Fix],
            Rotation.ShiftArray),
          shiftedRead,
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(
                LeftShift(
                  LeftShift(Read(`afile`), _, _, _, _, _),
                  structInnerInner,
                  IncludeId,
                  OnUndefined.Emit,
                  repairInnerInner,
                  Rotation.ShiftArray),
                structInner,
                ExcludeId,
                OnUndefined.Emit,
                repairInner,
                Rotation.ShiftMap),
              structOuter,
              ExcludeId,
              OnUndefined.Emit,
              repairOuter,
              Rotation.ShiftArray),
            fm) =>

          structInnerInner must beTreeEqual(recFunc.Hole)

          repairInnerInner must beTreeEqual(
            func.StaticMapS(
              "original" -> AccessLeftTarget[Fix](Access.value(_)),
              "results" -> RightTarget[Fix]))

          structInner must beTreeEqual(
            recFunc.ProjectKeyS(recFunc.Hole, "results"))

          repairInner must beTreeEqual(
            func.ConcatMaps(
              AccessLeftTarget[Fix](Access.value(_)),
              func.MakeMapS("results", RightTarget[Fix])))

          structOuter must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "results"))

          repairOuter must beTreeEqual(
            func.StaticMapS(
              "0" ->
                RightTarget[Fix],
              "1" ->
                func.ProjectKeyS(
                  AccessLeftTarget[Fix](Access.value(_)),
                  "original")))

          fm must beTreeEqual(
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "0"),
              recFunc.ProjectKeyS(recFunc.Hole, "1")))
      }
    }

    // c[*] / d[*][*]
    "coalesce uneven shifts" in {
      val cdivd =
        qsu.autojoin2((
          qsu.leftShift(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "c"),
            ExcludeId,
            OnUndefined.Omit,
            RightTarget[Fix],
            Rotation.ShiftArray),
          qsu.leftShift(
            qsu.leftShift(
              shiftedRead,
              recFunc.ProjectKeyS(recFunc.Hole, "d"),
              ExcludeId,
              OnUndefined.Omit,
              RightTarget[Fix],
              Rotation.ShiftArray),
            recFunc.Hole,
            ExcludeId,
            OnUndefined.Omit,
            RightTarget[Fix],
            Rotation.ShiftArray),
          _(MapFuncsCore.Divide(_, _))))

      val qgraph = QSUGraph.fromTree[Fix](cdivd)

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              MultiLeftShift(
                LeftShift(Read(_), _, _, _, _, _),
                List(
                  (cstruct, _, _),
                  (dstruct, _, _)),
                OnUndefined.Emit,
                innerRepair),
              outerStruct,
              _,
              OnUndefined.Emit,
              outerRepair,
              _),
            fm) =>

          cstruct must beTreeEqual(func.ProjectKeyS(func.Hole, "c"))
          dstruct must beTreeEqual(func.ProjectKeyS(func.Hole, "d"))

          innerRepair must beTreeEqual(
            func.StaticMapS(
              "left" ->
                func.MakeMapS("0", Free.pure[MapFunc, Access[Hole] \/ Int](0.right)),
              "right" ->
                Free.pure[MapFunc, Access[Hole] \/ Int](1.right)))

          outerStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "right"))

          outerRepair must beTreeEqual(
            func.ConcatMaps(
              func.ProjectKeyS(
                AccessLeftTarget[Fix](Access.value(_)),
                "left"),
              func.MakeMapS("1", RightTarget[Fix])))

          fm must beTreeEqual(
            recFunc.Divide(
              recFunc.ProjectKeyS(recFunc.Hole, "0"),
              recFunc.ProjectKeyS(recFunc.Hole, "1")))
      }
    }

    // a[*][*][*] + b - c[*] / d[*][*]
    "coalesce a thing that looks a lot like the search card" in {
      // a[*][*][*] + b
      val aplusb =
        qsu.autojoin2((
          qsu.leftShift(
            qsu.leftShift(
              qsu.leftShift(
                shiftedRead,
                recFunc.ProjectKeyS(recFunc.Hole, "a"),
                ExcludeId,
                OnUndefined.Omit,
                RightTarget[Fix],
                Rotation.ShiftArray),
              recFunc.Hole,
              ExcludeId,
              OnUndefined.Omit,
              RightTarget[Fix],
              Rotation.ShiftArray),
            recFunc.Hole,
            ExcludeId,
            OnUndefined.Omit,
            RightTarget[Fix],
            Rotation.ShiftArray),
          qsu.map((shiftedRead, recFunc.ProjectKeyS(recFunc.Hole, "b"))),
          _(MapFuncsCore.Add(_, _))))

      // c[*] / d[*][*]
      val cdivd =
        qsu.autojoin2((
          qsu.leftShift(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "c"),
            ExcludeId,
            OnUndefined.Omit,
            RightTarget[Fix],
            Rotation.ShiftArray),
          qsu.leftShift(
            qsu.leftShift(
              shiftedRead,
              recFunc.ProjectKeyS(recFunc.Hole, "d"),
              ExcludeId,
              OnUndefined.Omit,
              RightTarget[Fix],
              Rotation.ShiftArray),
            recFunc.Hole,
            ExcludeId,
            OnUndefined.Omit,
            RightTarget[Fix],
            Rotation.ShiftArray),
          _(MapFuncsCore.Divide(_, _))))

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((aplusb, cdivd, _(MapFuncsCore.Subtract(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              MultiLeftShift(
                MultiLeftShift(
                  LeftShift(Read(`afile`), _, _, _, _, _),
                  List(
                    (innerastruct, _, _),
                    (innercstruct, _, _),
                    (innerdstruct, _, _)),
                  OnUndefined.Emit,
                  innerMultiRepair),
                List(
                  (outerastruct, _, _),
                  (outerdstruct, _, _)),
                OnUndefined.Emit,
                outerMultiRepair),
              singleStruct,
              _,
              OnUndefined.Emit,
              singleRepair,
              _),
            fm) =>

        innerastruct must beTreeEqual(func.ProjectKeyS(func.Hole, "a"))
        innercstruct must beTreeEqual(func.ProjectKeyS(func.Hole, "c"))
        innerdstruct must beTreeEqual(func.ProjectKeyS(func.Hole, "d"))

        innerMultiRepair must beTreeEqual(
          func.StaticMapS(
            "left" -> func.StaticMapS(
              "left" -> func.StaticMapS(
                "original" ->
                  AccessHole[Fix].map(_.left[Int]),
                "results" ->
                  Free.pure[MapFunc, Access[Hole] \/ Int](0.right)),
              "right" ->
                func.MakeMapS("2", Free.pure[MapFunc, Access[Hole] \/ Int](1.right))),
            "right" ->
              Free.pure[MapFunc, Access[Hole] \/ Int](2.right)))

        outerastruct must beTreeEqual(
          func.ProjectKeyS(
            func.ProjectKeyS(
              func.ProjectKeyS(func.Hole, "left"),
              "left"),
            "results"))

        outerdstruct must beTreeEqual(func.ProjectKeyS(func.Hole, "right"))

        outerMultiRepair must beTreeEqual(
          func.StaticMapS(
            "left" ->
              func.ConcatMaps(
                func.ProjectKeyS(
                  AccessHole[Fix].map(_.left[Int]),
                  "left"),
                func.MakeMapS(
                  "left",
                  func.ConcatMaps(
                    func.ProjectKeyS(
                      func.ProjectKeyS(
                        AccessHole[Fix].map(_.left[Int]),
                        "left"),
                      "left"),
                    func.MakeMapS(
                      "results",
                      Free.pure[MapFunc, Access[Hole] \/ Int](0.right))))),
            "right" -> func.MakeMapS("3", Free.pure[MapFunc, Access[Hole] \/ Int](1.right))))

        singleRepair must beTreeEqual(
          func.ConcatMaps(
            func.ConcatMaps(
              func.StaticMapS(
                "0" ->
                  RightTarget[Fix],
                "1" ->
                  func.ProjectKeyS(
                    func.ProjectKeyS(
                      func.ProjectKeyS(
                        AccessLeftTarget[Fix](Access.value(_)),
                        "left"),
                      "left"),
                    "original")),
              func.ProjectKeyS(
                func.ProjectKeyS(
                  AccessLeftTarget[Fix](Access.value(_)),
                  "left"),
                "right")),
            func.ProjectKeyS(
              AccessLeftTarget[Fix](Access.value(_)),
              "right")))

        singleStruct must beTreeEqual(
          recFunc.ProjectKeyS(
            recFunc.ProjectKeyS(
              recFunc.ProjectKeyS(
                recFunc.Hole,
                "left"),
              "left"),
          "results"))

        fm must beTreeEqual(
          recFunc.Subtract(
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "0"),
              recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "1"), "b")),
            recFunc.Divide(
              recFunc.ProjectKeyS(recFunc.Hole, "2"),
              recFunc.ProjectKeyS(recFunc.Hole, "3"))))
      }
    }

    // [a, b[*][*]]]
    "coalesce with proper struct a contextual shift autojoin" in {
      val qgraph =
        QSUGraph.fromTree[Fix](
          qsu.autojoin2((
            qsu.map(
              shiftedRead,
              recFunc.MakeArray(
                recFunc.Guard(
                  recFunc.Hole,
                  Type.AnyObject,
                  recFunc.ProjectKeyS(recFunc.Hole, "a"),
                  recFunc.Undefined))),
            qsu.map(
              qsu.leftShift(
                qsu.leftShift(
                  qsu.map(
                    shiftedRead,
                    recFunc.Guard(
                      recFunc.Hole,
                      Type.AnyObject,
                      recFunc.ProjectKeyS(recFunc.Hole, "b"),
                      recFunc.Undefined)),
                  recFunc.Hole,
                  ExcludeId,
                  OnUndefined.Omit,
                  RightTarget[Fix],
                  Rotation.ShiftArray),
                recFunc.Hole,
                ExcludeId,
                OnUndefined.Omit,
                RightTarget[Fix],
                Rotation.ShiftArray),
              recFunc.MakeArray(recFunc.Hole)),
            _(MapFuncsCore.ConcatArrays(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(
                LeftShift(Read(_), _, _, _, _, _),
                innerStruct,
                _,
                OnUndefined.Emit,
                innerRepair,
                _),
              outerStruct,
              _,
              OnUndefined.Emit,
              outerRepair,
              _),
            fm) =>

          innerStruct must beTreeEqual(
            recFunc.Guard(
              recFunc.Hole,
              Type.AnyObject,
              recFunc.ProjectKeyS(recFunc.Hole, "b"),
              recFunc.Undefined))

          innerRepair must beTreeEqual(
            func.StaticMapS(
              "original" ->
                AccessLeftTarget[Fix](Access.value(_)),
              "results" ->
                RightTarget[Fix]))

          outerStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "results"))

          outerRepair must beTreeEqual(
            func.StaticMapS(
              "1" ->
                RightTarget[Fix],
              "0" ->
                func.ProjectKeyS(
                  AccessLeftTarget[Fix](Access.value(_)),
                  "original")))

          fm must beTreeEqual(
            recFunc.ConcatArrays(
              recFunc.MakeArray(
                recFunc.Guard(
                  recFunc.ProjectKeyS(recFunc.Hole, "0"),
                  Type.AnyObject,
                  recFunc.ProjectKeyS(
                    recFunc.ProjectKeyS(recFunc.Hole, "0"),
                    "a"),
                  recFunc.Undefined)),
              recFunc.MakeArray(recFunc.ProjectKeyS(recFunc.Hole, "1"))))
      }
    }

    // select [a, foo[*][*]] from afile
    "properly coalesce a more realistic double-shift example" in {
      val qsu0 = qsu.read(afile)
      val qsu2 = qsu.unreferenced()

      val qsu3 = qsu.map(qsu2, recFunc.Undefined[Hole])

      val qsu1 =
        qsu.leftShift(
          qsu0,
          recFunc.Hole,
          ExcludeId,
          OnUndefined.Omit,
          RightTarget[Fix],
          Rotation.ShiftMap)

      val qsu9 =
        qsu.map(qsu1, recFunc.ProjectKeyS(recFunc.Hole, "foo"))

      val qsu11 =
        qsu.leftShift(
          qsu9,
          recFunc.Hole,
          ExcludeId,
          OnUndefined.Omit,
          RightTarget[Fix],
          Rotation.FlattenArray)

      // this is the problematic node here
      // it forces the inductive minimization, since the
      // shifts are not directly consecutive, only merged by
      // a mappable region
      val qsu12 =
        qsu.autojoin3((
          qsu11,
          qsu11,
          qsu3,
          _(MapFuncsCore.Guard(_, Type.AnyArray, _, _))))

      val qsu6 =
        qsu.map(qsu1, recFunc.ProjectKeyS(recFunc.Hole, "a"))

      val qsu13 =
        qsu.leftShift(
          qsu12,
          recFunc.Hole,
          ExcludeId,
          OnUndefined.Omit,
          RightTarget[Fix],
          Rotation.FlattenArray)

      val qsu7 =
        qsu.map(qsu6, recFunc.MakeArray(recFunc.Hole))

      val qsu14 =
        qsu.map(qsu13, recFunc.MakeArray(recFunc.Hole))

      val qsu16 =
        qsu.autojoin2((
          qsu7,
          qsu14,
          _(MapFuncsCore.ConcatArrays(_, _))))

      val qgraph = QSUGraph.fromTree[Fix](qsu16)

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(
                LeftShift(Read(_), _, _, _, _, _),
                innerStruct,
                _,
                OnUndefined.Emit,
                innerRepair,
                _),
              outerStruct,
              _,
              OnUndefined.Emit,
              outerRepair,
              _),
            fm) => ok
      }
    }

    // a + b[*].c[*]
    "coalesce uneven shifts with an intervening map" in {
      val qgraph =
        QSUGraph.fromTree[Fix](
          qsu.autojoin2((
            qsu.map(shiftedRead, recFunc.ProjectKeyS(recFunc.Hole, "a")),
            qsu.leftShift(
              qsu.map(
                qsu.leftShift(
                  qsu.map(shiftedRead, recFunc.ProjectKeyS(recFunc.Hole, "b")),
                  recFunc.Hole,
                  ExcludeId,
                  OnUndefined.Omit,
                  RightTarget[Fix],
                  Rotation.FlattenArray),
                recFunc.ProjectKeyS(recFunc.Hole, "c")),
              recFunc.Hole,
              ExcludeId,
              OnUndefined.Omit,
              RightTarget[Fix],
              Rotation.FlattenArray),
            _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case
          Map(
            LeftShift(
              LeftShift(
                LeftShift(Read(_), _, _, _, _, _),
                innerStruct,
                _,
                _,
                innerRepair,
                _),
              outerStruct,
              _,
              _,
              outerRepair,
              _),
            fm) =>

          innerStruct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "b"))

          innerRepair must beTreeEqual(
            func.StaticMapS(
              "original" ->
                AccessLeftTarget[Fix](Access.value(_)),
              "results" ->
                RightTarget[Fix]))

          outerStruct must beTreeEqual(
            recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "results"), "c"))

          outerRepair must beTreeEqual(
            func.StaticMapS(
              "1" ->
                RightTarget[Fix],
              "0" ->
                func.ProjectKeyS(AccessLeftTarget[Fix](Access.value(_)), "original")))

          fm must beTreeEqual(
            recFunc.Add(
              recFunc.ProjectKeyS(
                recFunc.ProjectKeyS(recFunc.Hole, "0"),
                "a"),
              recFunc.ProjectKeyS(recFunc.Hole, "1")))
      }
    }

    // a[*] + b[*].c[*]
    "coalesce a shift with uneven shifts with an intervening map" in {
      val qgraph =
        QSUGraph.fromTree[Fix](
          qsu.autojoin2((
            qsu.leftShift(
              qsu.map(shiftedRead, recFunc.ProjectKeyS(recFunc.Hole, "a")),
              recFunc.Hole,
              ExcludeId,
              OnUndefined.Omit,
              RightTarget[Fix],
              Rotation.FlattenArray),
            qsu.leftShift(
              qsu.map(
                qsu.leftShift(
                  qsu.map(shiftedRead, recFunc.ProjectKeyS(recFunc.Hole, "b")),
                  recFunc.Hole,
                  ExcludeId,
                  OnUndefined.Omit,
                  RightTarget[Fix],
                  Rotation.FlattenArray),
                recFunc.ProjectKeyS(recFunc.Hole, "c")),
              recFunc.Hole,
              ExcludeId,
              OnUndefined.Omit,
              RightTarget[Fix],
              Rotation.FlattenArray),
            _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          LeftShift(
            MultiLeftShift(
              LeftShift(Read(_), _, _, _, _, _),
              List(
                (innerastruct, _, _),
                (innerbstruct, _, _)),
              _,
              innerRepair),
            outerStruct,
            _,
            _,
            outerRepair,
            _),
          fm) =>

          innerastruct must beTreeEqual(func.ProjectKeyS(func.Hole, "a"))
          innerbstruct must beTreeEqual(func.ProjectKeyS(func.Hole, "b"))

          innerRepair must beTreeEqual(
            func.StaticMapS(
              "left" ->
                func.StaticMapS(
                  "0" ->
                    Free.pure[MapFunc, Access[Hole] \/ Int](0.right)),
              "right" ->
                Free.pure[MapFunc, Access[Hole] \/ Int](1.right)))

          outerStruct must beTreeEqual(
            recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "right"), "c"))

          outerRepair must beTreeEqual(
            func.ConcatMaps(
              func.ProjectKeyS(
                AccessLeftTarget[Fix](Access.value(_)),
                "left"),
              func.MakeMapS("1", RightTarget[Fix])))

          fm must beTreeEqual(
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "0"),
              recFunc.ProjectKeyS(recFunc.Hole, "1")))
      }
    }

    // b[*] or b[*][*]
    "correctly coalesce uneven shifts of the same source" in {
      val shiftedRead =
        qsu.tread(afile)

      val guardedRead =
        qsu.autojoin3((
          shiftedRead,
          shiftedRead,
          qsu.undefined(),
          _(MapFuncsCore.Guard(_, Type.AnyObject, _, _))))

      val projectB =
        qsu.autojoin2((
          guardedRead,
          qsu.cstr("b"),
          _(MapFuncsCore.ProjectKey(_, _))))

      val innerShift =
        qsu.transpose(
          qsu.autojoin3((
            projectB,
            projectB,
            qsu.undefined(),
            _(MapFuncsCore.Guard(_, Type.FlexArr(0, None, Type.FlexArr(0, None, Type.Bool)), _, _)))),
          Retain.Values,
          Rotation.FlattenArray)

      val qgraph =
        QSUGraph.fromTree[Fix](
          qsu.map(
            qsu.qsFilter(
              qsu._autojoin2((
                guardedRead,
                qsu.autojoin2((
                  qsu.transpose(
                    qsu.autojoin3((
                      projectB,
                      projectB,
                      qsu.undefined(),
                      _(MapFuncsCore.Guard(_, Type.FlexArr(0, None, Type.Bool), _, _)))),
                    Retain.Values,
                    Rotation.FlattenArray),
                  qsu.transpose(
                    qsu.autojoin3((
                      innerShift,
                      innerShift,
                      qsu.undefined(),
                      _(MapFuncsCore.Guard(_, Type.FlexArr(0, None, Type.Bool), _, _)))),
                    Retain.Values,
                    Rotation.FlattenArray),
                  _(MapFuncsCore.Or(_, _)))),
                func.StaticMapS(
                  "filter_source" -> func.LeftSide,
                  "filter_predicate" -> func.RightSide))),
              recFunc.ProjectKeyS(recFunc.Hole, "filter_predicate")),
            recFunc.ProjectKeyS(recFunc.Hole, "filter_source")))

      val leftShiftCount =
        runOn(qgraph).foldMapUp {
          case LeftShift(_, _, _, _, _, _) => 1
          case MultiLeftShift(_, ss, _, _) => ss.length
          case _                           => 0
        }

      // TODO: Should really be 3, but another bug is duplicating the inner
      //       shift common to both sides of the `Or`
      leftShiftCount must_= 3
    }

    "create a single AutoJoin2 when there are exactly two sources" in {
      val reduce =
        qsu.qsReduce(
          shiftedRead,
          Nil,
          List(ReduceFuncs.Arbitrary(func.ProjectKeyS(func.Hole, "b"))),
          Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0))))

      val twoSources =
        QSUGraph.fromTree[Fix](
          qsu._autojoin2(
            (qsu._autojoin2(
              (qsu.map(shiftedRead, recFunc.MakeMapS("a", recFunc.ProjectKeyS(recFunc.Hole, "a"))),
                qsu.map(reduce, recFunc.MakeMapS("b", recFunc.Hole)),
                func.ConcatMaps(func.LeftSide, func.RightSide))),
              qsu.map(shiftedRead, recFunc.MakeMapS("c", recFunc.ProjectKeyS(recFunc.Hole, "c"))),
              func.ConcatMaps(func.LeftSide, func.RightSide))))

      runOn(twoSources) must beLike {
        case AutoJoin2(
          LeftShift(Read(_), _, _, _, _, _),
          QSReduce(_, _, _, _),
          repair) =>

          repair must beTreeEqual(
            func.StaticMapS(
              "a" -> func.ProjectKeyS(func.LeftSide, "a"),
              "b" -> func.RightSide,
              "c" -> func.ProjectKeyS(func.LeftSide, "c")))
      }
    }

    // a{:_}, a{_:}
    "shifts keys and values with a single left-shift" in {
      val shifts = QSUGraph.fromTree[Fix](
        qsu._autojoin2((
          qsu.leftShift(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "a"),
            IdOnly,
            OnUndefined.Emit,
            RightTarget[Fix],
            Rotation.ShiftMap),
          qsu.leftShift(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "a"),
            ExcludeId,
            OnUndefined.Emit,
            RightTarget[Fix],
            Rotation.ShiftMap),
          func.StaticMapS(
            "0" -> func.LeftSide,
            "1" -> func.RightSide))))

      runOn(shifts) must beLike {
        case Map(
          LeftShift(
            LeftShift(Read(_), _, _, _, _, _),
            struct,
            IncludeId,
            OnUndefined.Emit,
            repair,
            Rotation.ShiftMap), outerMap) =>

          struct must beTreeEqual(recFunc.ProjectKeyS(recFunc.Hole, "a"))

          repair must beTreeEqual(
            func.StaticMapS(
              "0" -> func.ProjectIndexI(RightTarget[Fix], 0),
              "1" -> func.ProjectIndexI(RightTarget[Fix], 1)))

          outerMap must beTreeEqual(
            recFunc.StaticMapS(
              "0" -> recFunc.ProjectKeyS(recFunc.Hole, "0"),
              "1" -> recFunc.ProjectKeyS(recFunc.Hole, "1")))
      }
    }

    // a{:_}, b[*], a{_:}
    "reorders candidates to coalesce compatible shifts" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu._autojoin3((
          qsu.leftShift(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "a"),
            IdOnly,
            OnUndefined.Emit,
            RightTarget[Fix],
            Rotation.ShiftMap),
          qsu.leftShift(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "b"),
            IdOnly,
            OnUndefined.Emit,
            RightTarget[Fix],
            Rotation.ShiftArray),
          qsu.leftShift(
            shiftedRead,
            recFunc.ProjectKeyS(recFunc.Hole, "a"),
            ExcludeId,
            OnUndefined.Emit,
            RightTarget[Fix],
            Rotation.ShiftMap),
          func.StaticMapS(
            "0" -> func.LeftSide3,
            "1" -> func.Center,
            "2" -> func.RightSide3))))

      runOn(qgraph) must beLike {
        case Map(
          MultiLeftShift(
            LeftShift(Read(_), _, _, _, _, _),
            List((structA, IncludeId, Rotation.ShiftMap), (structB, IdOnly, Rotation.ShiftArray)),
            OnUndefined.Emit,
            repair), outerMap) =>

          structA must beTreeEqual(func.ProjectKeyS(func.Hole, "a"))
          structB must beTreeEqual(func.ProjectKeyS(func.Hole, "b"))

          repair must beTreeEqual(
            func.StaticMapS(
              "0" -> func.ProjectIndexI(0.right[Access[Hole]].pure[FreeMapA], 0),
              "2" -> func.ProjectIndexI(0.right[Access[Hole]].pure[FreeMapA], 1),
              "1" -> 1.right[Access[Hole]].pure[FreeMapA]))

          outerMap must beTreeEqual(
            recFunc.StaticMapS(
              "0" -> recFunc.ProjectKeyS(recFunc.Hole, "0"),
              "1" -> recFunc.ProjectKeyS(recFunc.Hole, "1"),
              "2" -> recFunc.ProjectKeyS(recFunc.Hole, "2")))
      }
    }
  }

  def runOn(qgraph: QSUGraph): QSUGraph =
    runOn_(qgraph).graph

  def runOn_(qgraph: QSUGraph): AuthenticatedQSU[Fix] = {
    val resultsF = for {
      agraph0 <- ApplyProvenance[Fix, F](qgraph)
      agraph <- ReifyBuckets[Fix, F](agraph0)
      back <- MinimizeAutoJoins[Fix, F](agraph)
    } yield back

    val results = resultsF.run.eval(0L).value.toEither
    results must beRight

    results.right.get
  }
}
