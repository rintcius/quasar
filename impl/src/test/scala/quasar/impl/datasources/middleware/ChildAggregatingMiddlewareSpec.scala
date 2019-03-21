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

package quasar.impl.datasource

import slamdata.Predef.{Stream => _, _}
import quasar._
import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.api.table.ColumnType
import quasar.common.{CPath, CPathField}
import quasar.connector.ResourceError
import quasar.contrib.iota._
import quasar.contrib.scalaz.MonadError_
import quasar.ejson.EJson
import quasar.fp._
import quasar.impl.datasources.middleware.ChildAggregatingMiddleware
import quasar.qscript.{InterpretedRead, RecFreeMap, construction}
import quasar.ScalarStage.{Cartesian, Mask, Project, Wrap}

import cats.effect.IO
import matryoshka.{Hole => _, _}
import matryoshka.data.Fix
import matryoshka.data.free._
import shims._

class ChildAggregatingMiddlewareSpec extends Qspec with TreeMatchers {

  sequential //TODO temp

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val someSourcePath = ResourcePath.root() / ResourceName("some")  / ResourceName("src")  / ResourceName("path")
  val dontCare = ResourcePath.root() / ResourceName("dontcare")

  val s = ResourcePath.root() / ResourceName("source")
  val v = ResourcePath.root() / ResourceName("value")

  val rec = construction.RecFunc[Fix]
  val json = ejson.Fixed[Fix[EJson]]

  "no instructions -> map with source and value" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List()),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, List())),
      rec.StaticMapS(
        "source" -> rec.Constant(json.str("/some/src/path")),
        "value" -> rec.Hole))
  }

  "project a non-existing path -> undefined" in {
    // gen any other than source/value
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Project(CPath.parse(("nonExisting"))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, List())),
      rec.Undefined)
  }

  "project source -> the src path" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Project(CPath.parse(("source"))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, List())),
      rec.Constant(json.str("/some/src/path")))
  }

  "project source.something -> undefined" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Project(CPath.parse(("source.something"))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, List())),
      rec.Undefined)
  }

  "project value -> hole" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Project(CPath.parse(("value"))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, Nil)),
      rec.Hole)
  }

  "project value.something -> project something on hole" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Project(CPath.parse(("value.something"))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, List(Project(CPath.parse(("something")))))),
      rec.Hole)
  }

  "project value, then some instr -> some instr on chole" in {
    // gen any instr
    val someInstruction = Wrap("name")
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Project(CPath.parse(("value"))), someInstruction)),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, List(someInstruction))),
      rec.Hole)
  }

  "project value.something, then some instr -> project something, then some instr on hole" in {
    // gen any instr
    val someInstruction = Wrap("name")
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Project(CPath.parse(("value.something"))), someInstruction)),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, List(Project(CPath.parse(("something"))), someInstruction))),
      rec.Hole)
  }

  "project source, then wrap -> map with wrap containing src path" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Project(CPath.parse(("source"))), Wrap("wrap"))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, Nil)),
      rec.StaticMapS("wrap" -> rec.Constant(json.str("/some/src/path"))))
  }

  "wrap -> wrap on map with source and value" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Wrap("name"))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, Nil)),
      rec.StaticMapS("name" ->
        rec.StaticMapS(
          "source" -> rec.Constant(json.str("/some/src/path")),
          "value" -> rec.Hole)))
  }

  "2 wraps -> wrap them as static map" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Wrap("first"), Wrap("second"))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, Nil)),
      rec.StaticMapS("second" ->
        rec.StaticMapS("first" ->
          rec.StaticMapS(
            "source" -> rec.Constant(json.str("/some/src/path")),
            "value" -> rec.Hole)
        )))
  }

  "mask source (string) -> map with source" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Mask(Map(CPath.parse("source") -> Set(ColumnType.String))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, Nil)),
      rec.StaticMapS("source" -> rec.Constant(json.str("/some/src/path"))))
  }

  "mask source (non string) -> undefined" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Mask(Map(CPath.parse("source") -> Set(ColumnType.Number))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, Nil)),
      rec.Undefined)
  }

  "mask value -> mask . on map with value" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Mask(Map(CPath.parse("value") -> Set(ColumnType.String))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, List(Mask(Map(CPath.parse("") -> Set(ColumnType.String)))))),
      rec.StaticMapS("value" -> rec.Hole))
  }

  "mask value.something -> mask something on map with value" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Mask(Map(CPath.parse("value.something") -> Set(ColumnType.String))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, List(Mask(Map(CPath.parse("something") -> Set(ColumnType.String)))))),
      rec.StaticMapS("value" -> rec.Hole))
  }

  "mask empty -> undefined" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Mask(Map()))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, Nil)),
      rec.Undefined)
  }

  "cartesian of source as s -> map with s containing source" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Cartesian(Map((CPathField("s") -> ((CPathField("source"), Nil))))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, Nil)),
      rec.StaticMapS("s" -> rec.Constant(json.str("/some/src/path"))))
  }

  "cartesian of source as s0, source as s1 -> map with s0 and s1 containing source" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Cartesian(Map(
        (CPathField("s0") -> ((CPathField("source"), Nil))),
        (CPathField("s1") -> ((CPathField("source"), Nil))))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, Nil)),
      rec.StaticMapS(
        "s1" -> rec.Constant(json.str("/some/src/path")),
        "s0" -> rec.Constant(json.str("/some/src/path"))))
  }

  "cartesian of value as v -> map with v containing hole" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Cartesian(Map((CPathField("v") -> ((CPathField("value"), Nil))))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, Nil)),
      rec.StaticMapS("v" -> rec.Hole))
  }

  "cartesian of value as v0, value as v1 -> wrap, then transformed cartesian on hole" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId,
        List(Cartesian(Map(
          (CPathField("v0") -> ((CPathField("value"), Nil))),
          (CPathField("v1") -> ((CPathField("value"), Nil))))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId,
        List(
          Wrap("cartesian_value_wrap"),
          Cartesian(Map(
            (CPathField("v0") -> ((CPathField("cartesian_value_wrap"), Nil))),
            (CPathField("v1") -> ((CPathField("cartesian_value_wrap"), Nil)))))))),
      rec.Hole)
  }

  "cartesian of source as s, value as v -> map with s0 and s1 containing source" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Cartesian(Map(
        (CPathField("s") -> ((CPathField("source"), Nil))),
        (CPathField("v") -> ((CPathField("value"), Nil))))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, Nil)),
      rec.StaticMapS(
        "s" -> rec.Constant(json.str("/some/src/path")),
        "v" -> rec.Hole))
  }

  "cartesian of source as s, value.y as y, value as v -> transformed cartesian, map with hole plus s containing source" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId, List(Cartesian(Map(
        (CPathField("s") -> ((CPathField("source"), Nil))),
        (CPathField("y") -> ((CPathField("value"), Project(CPath.parse("y")) :: Nil))),
        (CPathField("v") -> ((CPathField("value"), Mask(Map(CPath.parse(".") -> Set(ColumnType.Object))) :: Nil))))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, List(
        Wrap("cartesian_value_wrap"),
        Cartesian(Map(
          (CPathField("y") -> ((CPathField("cartesian_value_wrap"), Project(CPath.parse("y")) :: Nil))),
          (CPathField("v") -> ((CPathField("cartesian_value_wrap"), Mask(Map(CPath.parse(".") -> Set(ColumnType.Object))) :: Nil)))))))),
      rec.ConcatMaps(
        rec.Hole,
        rec.StaticMapS("s" -> rec.Constant(json.str("/some/src/path")))))
  }

  "mask value top, then cartesian of value as v0, value as v1 -> transformed cartesian on map with wrapper containing hole" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId,
        List(
          Mask(Map(CPath.parse("value") -> ColumnType.Top)),
          Cartesian(Map(
            (CPathField("v0") -> ((CPathField("value"), Nil))),
            (CPathField("v1") -> ((CPathField("value"), Nil))))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId,
        List(
          Wrap("cartesian_value_wrap"),
          Cartesian(Map(
            (CPathField("v0") -> ((CPathField("cartesian_value_wrap"), Nil))),
            (CPathField("v1") -> ((CPathField("cartesian_value_wrap"), Nil)))))))),
      rec.Hole)
  }

  "mask source top and value.x top, then cartesian of source as s, value as v -> transformed cartesian on map with wrapper containing hole" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId,
        List(
          Mask(Map(
            CPath.parse("source") -> ColumnType.Top,
            CPath.parse("value.x") -> ColumnType.Top)),
          Cartesian(Map(
            (CPathField("s") -> ((CPathField("source"), Nil))),
            (CPathField("v") -> ((CPathField("value"), Nil))))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId,
        List(Mask(Map(CPath.parse("x") -> ColumnType.Top))))),
      rec.StaticMapS(
        "s" -> rec.Constant(json.str("/some/src/path")),
        "v" -> rec.Hole))
  }

  "wrap something, then cartesian of something.source as s, something.value as v -> map with s and v" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId,
        List(
          Wrap("something"),
          Cartesian(Map(
            (CPathField("s") -> ((CPathField("something"), Project(CPath.parse("source")) :: Nil))),
            (CPathField("v") -> ((CPathField("something"), Project(CPath.parse("value")) :: Nil))))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, Nil)),
      rec.StaticMapS(
        "s" -> rec.Constant(json.str("/some/src/path")),
        "v" -> rec.Hole))
  }

  "wrap something, then cartesian of something as b -> transformed cartesian wrapping value, map with b containing source and hole" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId,
        List(
          Wrap("something"),
          Cartesian(Map(
            (CPathField("b") -> ((CPathField("something"), Nil))))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, Wrap("value") :: Nil)),
      rec.StaticMapS(
        "b" -> rec.ConcatMaps(
            rec.StaticMapS("source" -> rec.Constant(json.str("/some/src/path"))),
            rec.Hole)))
  }

  "wrap something, then cartesian of something.source as s, something.value as v, something as x -> concat maps merging transformed cartesian with static map of source part" in {
    testTemplate(
      ScalarStages(IdStatus.ExcludeId,
        List(
          Wrap("something"),
          Cartesian(Map(
            (CPathField("s") -> ((CPathField("something"), Project(CPath.parse("source")) :: Nil))),
            (CPathField("v") -> ((CPathField("something"), Project(CPath.parse("value")) :: Nil))),
            (CPathField("x") -> ((CPathField("something"), Nil))))))),
      someSourcePath,
      InterpretedRead(someSourcePath, ScalarStages(IdStatus.ExcludeId, List(
        Wrap("cartesian_value_wrap"),
        Cartesian(Map(
          (CPathField("v") -> ((CPathField("cartesian_value_wrap"), Nil))),
          (CPathField("x") -> ((CPathField("cartesian_value_wrap"), Wrap("value") :: Nil)))))))),
      rec.ConcatMaps(
        rec.ConcatMaps(
          rec.Hole,
          rec.StaticMapS("x" -> rec.StaticMapS("source" -> rec.Constant(json.str("/some/src/path"))))),
        rec.StaticMapS("s" -> rec.Constant(json.str("/some/src/path")))))
  }

  def testTemplate(
      stages: ScalarStages,
      path: ResourcePath,
      expected: InterpretedRead[ResourcePath],
      expectedFm: RecFreeMap[Fix]) = {

    val (outIR, fm) =
      ChildAggregatingMiddleware.rewriteInstructions("source", "value")(InterpretedRead(dontCare, stages), path)

    fm must beTreeEqual(expectedFm)
    outIR must_=== expected
  }
}
