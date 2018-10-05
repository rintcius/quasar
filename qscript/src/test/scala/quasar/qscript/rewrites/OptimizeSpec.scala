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

package quasar.qscript.rewrites

import slamdata.Predef._

import quasar.Qspec
import quasar.contrib.iota._
import quasar.contrib.pathy._
import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.qscript._

import iotaz.{CopK, TNilK}
import iotaz.TListK.:::
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz.Const
import scalaz.std.option._

object OptimizeSpec extends Qspec {

  type QSBase =
    QScriptCore[Fix, ?]          :::
    EquiJoin[Fix, ?]             :::
    Const[ShiftedRead[AFile], ?] :::
    TNilK

  // initial QScript
  type QS[A] = CopK[QSBase, A]

  implicit val QS: Injectable[QS, QScriptTotal[Fix, ?]] =
    SubInject[QS, QScriptTotal[Fix, ?]]

  val qsDsl = construction.mkDefaults[Fix, QS]
  val fix = qsDsl.fix
  val recFunc = qsDsl.recFunc

  // initial QScript with ExtraShiftedRead
  type QSExtra[A] = CopK[Const[ExtraShiftedRead[AFile], ?] ::: QSBase, A]

  implicit val QSExtra: Injectable[QSExtra, QScriptTotal[Fix, ?]] =
    SubInject[QSExtra, QScriptTotal[Fix, ?]]

  val qsExtraDsl = construction.mkDefaults[Fix, QSExtra]
  val fixE = qsExtraDsl.fix
  val recFuncE = qsExtraDsl.recFunc
  val funcE = qsExtraDsl.func

  val ejs = Fixed[Fix[EJson]]

  val optimize = new Optimize[Fix]

  "path-finding" >> {

    "find the path of Hole" >> {
      optimize.findPath(funcE.Hole) must equal(
        Some(ShiftPath(List[String]())))
    }

    "find the path of a single object projection" >> {
      optimize.findPath(funcE.ProjectKeyS(funcE.Hole, "xyz")) must equal(
        Some(ShiftPath(List[String]("xyz"))))
    }

    "find the path of a triple object projection" >> {
      val fm =
        funcE.ProjectKeyS(
          funcE.ProjectKeyS(
            funcE.ProjectKeyS(
              funcE.Hole,
              "aaa"),
            "bbb"),
          "ccc")

      optimize.findPath(fm) must equal(
        Some(ShiftPath(List[String]("aaa", "bbb", "ccc"))))
    }

    "fail find the path of an object projection with a non-string key" >> {
      val fm =
        funcE.ProjectKey(funcE.Hole, funcE.Constant(ejs.bool(true)))

      optimize.findPath(fm) must equal(None)
    }

    "fail find the path of an object projection with a dynamic key" >> {
      val fm =
        funcE.ProjectKey(
          funcE.Hole,
          funcE.ToString(funcE.ProjectKeyS(funcE.Hole, "foobar")))

      optimize.findPath(fm) must equal(None)
    }

    "fail find the path of an object projection with a dynamic key that is itself a projection" >> {
      val fm =
        funcE.ProjectKey(
          funcE.Hole,
          funcE.ProjectKeyS(funcE.Hole, "foobar"))

      optimize.findPath(fm) must equal(None)
    }

    "fail to find the path of non-projection" >> {
      val fm =
        funcE.ProjectKeyS(
          funcE.MakeMapS(
            "map key",
            funcE.ProjectKeyS(funcE.Hole, "aaa")),
          "ccc")

      optimize.findPath(fm) must equal(None)
    }

    "fail to find the path of an array projection" >> {
      val fm =
        funcE.ProjectKeyS(
          funcE.ProjectIndexI(
            funcE.ProjectKeyS(
              funcE.Hole,
              "aaa"),
            42),
          "ccc")

      optimize.findPath(fm) must equal(None)
    }

    "fail to find the path of a projection whose source is not Hole" >> {
      val fm =
        funcE.ProjectKeyS(
          funcE.ProjectKeyS(
            funcE.ProjectKeyS(
              funcE.Constant[Hole](ejs.str("constant string")),
              "aaa"),
            "bbb"),
          "ccc")

      optimize.findPath(fm) must equal(None)
    }
  }

  "ExtraLeftShift rewrite" >> {

    val extraShiftFunc: QSExtra[Fix[QSExtra]] => QSExtra[Fix[QSExtra]] =
      liftFG[QScriptCore[Fix, ?], QSExtra, Fix[QSExtra]](optimize.extraShift[QSExtra, AFile])

    def extraShift(expr: Fix[QSExtra]): Fix[QSExtra] =
      expr.transCata[Fix[QSExtra]](extraShiftFunc)

    "rewrite when the read is shifted at the top-level and only the shifted values are referenced" >> {
      "with IncludeId" >> {
        val initial: Fix[QSExtra] =
          fixE.LeftShift(
            fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
            recFuncE.Hole,
            IncludeId, // IncludeId
            ShiftType.Map,
            OnUndefined.Omit,
            funcE.ConcatMaps(
              funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
              funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

        val expected: Fix[QSExtra] =
          fixE.Map(
            fixE.ExtraShiftedRead[AFile](
              rootDir </> file("foo"),
              ShiftPath(List()),
              IncludeId,
              ShiftType.Map,
              ShiftKey(ShiftedKey)),
            recFuncE.ConcatMaps(
              recFuncE.MakeMapS("k1",
                recFuncE.ProjectIndexI(recFuncE.ProjectKeyS(recFuncE.Hole, ShiftedKey), 0)),
              recFuncE.MakeMapS("v1",
                recFuncE.ProjectIndexI(recFuncE.ProjectKeyS(recFuncE.Hole, ShiftedKey), 1))))

        extraShift(initial) must equal(expected)
      }

      "with IdOnly" >> {
        val initial: Fix[QSExtra] =
          fixE.LeftShift(
            fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
            recFuncE.Hole,
            IdOnly, // IdOnly
            ShiftType.Map,
            OnUndefined.Omit,
            funcE.MakeMapS("k1", funcE.RightSide))

        val expected: Fix[QSExtra] =
          fixE.Map(
            fixE.ExtraShiftedRead[AFile](
              rootDir </> file("foo"),
              ShiftPath(List()),
              IdOnly,
              ShiftType.Map,
              ShiftKey(ShiftedKey)),
            recFuncE.MakeMapS("k1",
              recFuncE.ProjectKeyS(recFuncE.Hole, ShiftedKey)))

        extraShift(initial) must equal(expected)
      }

      "with ExcludeId" >> {
        val initial: Fix[QSExtra] =
          fixE.LeftShift(
            fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
            recFuncE.Hole,
            ExcludeId, // ExcludeId
            ShiftType.Map,
            OnUndefined.Omit,
            funcE.MakeMapS("v1", funcE.RightSide))

        val expected: Fix[QSExtra] =
          fixE.Map(
            fixE.ExtraShiftedRead[AFile](
              rootDir </> file("foo"),
              ShiftPath(List()),
              ExcludeId,
              ShiftType.Map,
              ShiftKey(ShiftedKey)),
            recFuncE.MakeMapS("v1",
              recFuncE.ProjectKeyS(recFuncE.Hole, ShiftedKey)))

        extraShift(initial) must equal(expected)
      }
    }

    "rewrite when the target LeftShift is the source of another node" >> {
      val initial: Fix[QSExtra] =
        fixE.Filter(
          fixE.LeftShift(
            fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
            recFuncE.Hole,
            ExcludeId, // ExcludeId
            ShiftType.Map,
            OnUndefined.Omit,
            funcE.MakeMapS("v1", funcE.RightSide)),
          recFuncE.Constant(ejs.bool(true)))

      val expected: Fix[QSExtra] =
        fixE.Filter(
          fixE.Map(
            fixE.ExtraShiftedRead[AFile](
              rootDir </> file("foo"),
              ShiftPath(List()),
              ExcludeId,
              ShiftType.Map,
              ShiftKey(ShiftedKey)),
            recFuncE.MakeMapS("v1",
              recFuncE.ProjectKeyS(recFuncE.Hole, ShiftedKey))),
          recFuncE.Constant(ejs.bool(true)))

      extraShift(initial) must equal(expected)
    }

    "rewrite when the shift source is a single projection" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
          recFuncE.ProjectKeyS(recFunc.Hole, "xyz"), // shift source is a single projection
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

        val expected: Fix[QSExtra] =
          fixE.Map(
            fixE.ExtraShiftedRead[AFile](
              rootDir </> file("foo"),
              ShiftPath(List("xyz")),
              IncludeId,
              ShiftType.Map,
              ShiftKey(ShiftedKey)),
            recFuncE.ConcatMaps(
              recFuncE.MakeMapS("k1",
                recFuncE.ProjectIndexI(recFuncE.ProjectKeyS(recFuncE.Hole, ShiftedKey), 0)),
              recFuncE.MakeMapS("v1",
                recFuncE.ProjectIndexI(recFuncE.ProjectKeyS(recFuncE.Hole, ShiftedKey), 1))))

      extraShift(initial) must equal(expected)
    }

    "rewrite when the shift source is three projections" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
          recFuncE.ProjectKeyS( // shift source is three projections
            recFuncE.ProjectKeyS(
              recFuncE.ProjectKeyS(recFunc.Hole,
                "aaa"),
              "bbb"),
            "ccc"),
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

        val expected: Fix[QSExtra] =
          fixE.Map(
            fixE.ExtraShiftedRead[AFile](
              rootDir </> file("foo"),
              ShiftPath(List("aaa", "bbb", "ccc")),
              IncludeId,
              ShiftType.Map,
              ShiftKey(ShiftedKey)),
            recFuncE.ConcatMaps(
              recFuncE.MakeMapS("k1",
                recFuncE.ProjectIndexI(recFuncE.ProjectKeyS(recFuncE.Hole, ShiftedKey), 0)),
              recFuncE.MakeMapS("v1",
                recFuncE.ProjectIndexI(recFuncE.ProjectKeyS(recFuncE.Hole, ShiftedKey), 1))))

      extraShift(initial) must equal(expected)
    }

    "not rewrite when the shift struct is a (nonsensical) constant" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
          recFuncE.Constant(ejs.str("string!")), // constant string
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

      extraShift(initial) must equal(initial)
    }

    // we can support projection through arrays in the future.
    // 🦖  rawr!
    "not rewrite when the shift struct contains static array and object projections" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
          recFuncE.ProjectKeyS(
            recFuncE.ProjectIndexI( // shift source contains an array projection
              recFuncE.ProjectKeyS(recFunc.Hole,
                "aaa"),
              42),
            "ccc"),
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

      extraShift(initial) must equal(initial)
    }

    // we will need to allow certain static mapfuncs as structs in the future.
    // 🦕  herbivore rawr!
    "not rewrite when the shift struct is not a projection and not Hole" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
          recFuncE.ProjectKeyS(
            recFuncE.MakeMapS( // shift source is not a projection
              "i am a key",
              recFuncE.ProjectKeyS(recFunc.Hole, "aaa")),
            "ccc"),
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

      extraShift(initial) must equal(initial)
    }

    "not rewrite when the non-shifted data is referenced (via LeftSide)" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
          recFunc.Hole,
          IncludeId,
          ShiftType.Map,
          OnUndefined.Emit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.LeftSide))) // LeftSide is referenced

      extraShift(initial) must equal(initial)
    }

    "not rewrite when the ShiftedRead has IncludeId" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.ShiftedRead[AFile](rootDir </> file("foo"), IncludeId), // IncludeId
          recFunc.Hole,
          IncludeId,
          ShiftType.Map,
          OnUndefined.Emit,
          funcE.RightSide)

      extraShift(initial) must equal(initial)
    }

    "not rewrite when the ShiftedRead has IdOnly" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.ShiftedRead[AFile](rootDir </> file("foo"), IdOnly), // IdOnly
          recFunc.Hole,
          IncludeId,
          ShiftType.Map,
          OnUndefined.Emit,
          funcE.RightSide)

      extraShift(initial) must equal(initial)
    }
  }

  "no-op Map rewrite" >> {

    val elideNoopMapFunc: QS[Fix[QS]] => QS[Fix[QS]] =
      liftFG[QScriptCore[Fix, ?], QS, Fix[QS]](optimize.elideNoopMap[QS])

    def elideNoopMap(expr: Fix[QS]): Fix[QS] =
      expr.transCata[Fix[QS]](elideNoopMapFunc)

    "elide outer no-op Map" >> {
      val src: Fix[QS] =
        fix.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId)

      elideNoopMap(fix.Map(src, recFunc.Hole)) must equal(src)
    }

    "elide nested no-op Map" >> {
      val src: Fix[QS] =
        fix.Map(
          fix.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
          recFunc.ProjectKeyS(recFunc.Hole, "bar"))

      val qs: Fix[QS] =
        fix.Filter(
          fix.Map(src, recFunc.Hole),
          recFunc.ProjectKeyS(recFunc.Hole, "baz"))

      val expected: Fix[QS] =
        fix.Filter(
          src,
          recFunc.ProjectKeyS(recFunc.Hole, "baz"))

      elideNoopMap(qs) must equal(expected)
    }

    "elide double no-op Map" >> {
      val src: Fix[QS] =
        fix.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId)

      elideNoopMap(fix.Map(fix.Map(src, recFunc.Hole), recFunc.Hole)) must equal(src)
    }
  }
}
