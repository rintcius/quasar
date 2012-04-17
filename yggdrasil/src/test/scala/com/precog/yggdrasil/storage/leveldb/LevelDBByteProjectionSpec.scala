/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil
package leveldb

import org.scalacheck.{Arbitrary, Gen}
import Gen._
import Arbitrary.arbitrary
import org.scalacheck.Prop
import org.scalacheck.Test.Params
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import scala.collection.immutable.ListMap

import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.common._
import com.precog.common.util._

import LevelDBByteProjectionSpec._

import blueeyes.json.JPath

import scalaz._


object LevelDBByteProjectionSpec {
  val cvInt = CInt(4)
  val cvLong = CLong(5)
  val cvString = CString("string")
  val cvBoolean = CBoolean(true)
  val cvFloat = CFloat(6)
  val cvDouble = CDouble(7)
  val cvDouble2 = CDouble(9)
  val cvNum = CNum(8)
//  val cvEmptyArray = CEmptyArray
//  val cvEmptyObject = CEmptyObject
//  val cvNull = CNull

  val colDesStringFixed: ColumnDescriptor = ColumnDescriptor(Path("path5"), JPath("key5"), CStringFixed(1), Authorities(Set()))
  val colDesStringArbitrary: ColumnDescriptor = ColumnDescriptor(Path("path6"), JPath("key6"), CStringArbitrary, Authorities(Set()))
  val colDesBoolean: ColumnDescriptor = ColumnDescriptor(Path("path0"), JPath("key0"), CBoolean, Authorities(Set()))
  val colDesInt: ColumnDescriptor = ColumnDescriptor(Path("path1"), JPath("key1"), CInt, Authorities(Set()))
  val colDesLong: ColumnDescriptor = ColumnDescriptor(Path("path2"), JPath("key2"), CLong, Authorities(Set()))
  val colDesFloat: ColumnDescriptor = ColumnDescriptor(Path("path3"), JPath("key3"), CFloat, Authorities(Set()))
  val colDesDouble: ColumnDescriptor = ColumnDescriptor(Path("path4"), JPath("key4"), CDouble, Authorities(Set()))
  val colDesDouble2: ColumnDescriptor = ColumnDescriptor(Path("path8"), JPath("key8"), CDouble, Authorities(Set()))  
  val colDesDecimal: ColumnDescriptor = ColumnDescriptor(Path("path7"), JPath("key7"), CDecimalArbitrary, Authorities(Set()))

  def byteProjectionInstance(indexedColumns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) = { 
    ProjectionDescriptor(indexedColumns, sorting) map { d => 
      new LevelDBByteProjection {
        val descriptor: ProjectionDescriptor = d
      }
    }
  }

  def byteProjectionInstance2(desc: ProjectionDescriptor) = {
    desc map { desc =>
      new LevelDBByteProjection {
        val descriptor: ProjectionDescriptor = desc
      }
    }
  }

  def constructByteProjection(columns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) = {
    byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }
  }
  
  def constructByteProjection2(desc: ProjectionDescriptor) = {
    byteProjectionInstance2(desc) ///||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }
  }

  def constructIds(uniqueIndices: Int): Vector[Long] = {  
    Gen.listOfN(uniqueIndices, Arbitrary.arbitrary[Long]).sample.get.foldLeft(Vector.empty: Vector[Long])((v, id) => v :+ id)
  }

  def constructValues(listOfColDes: List[ColumnDescriptor]): Seq[CValue] = {
    val listOfTypes = listOfColDes.foldLeft(List.empty: List[CType])((l,col) => l :+ col.valueType)
    listOfTypes.foldLeft(Seq.empty: Seq[CValue]) {
      case (seq, CInt)  => seq :+ cvInt
      case (seq, CFloat) => seq :+ cvFloat
      case (seq, CBoolean) => seq :+ cvBoolean
      case (seq, CDouble) => seq :+ cvDouble
      case (seq, CLong) => seq :+ cvLong
      case (seq, CDecimalArbitrary) => seq :+ cvNum
      case (seq, CStringArbitrary) => seq :+ cvString
      case (seq, CEmptyArray) => seq :+ null
      case (seq, CEmptyObject) => seq :+ null
      case (seq, CNull) => seq :+ null
    }
  }
}

class LevelDBByteProjectionSpec extends Specification with ScalaCheck {
  "a byte projection generated from sample data" should {
    "return the arguments of project, when unproject is applied to project" in {
      val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable
      val dataPath = Path("/test")
      implicit val (sampleData, _) = DistributedSampleSet.sample(5, 0)

      val projDes: List[ProjectionDescriptor] = {
        sampleData.zipWithIndex.foldLeft(List.empty[ProjectionDescriptor]) {
          case (acc, (jobj, i)) => routingTable.route(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
            case (acc, ProjectionData(descriptor, identities, values, _)) =>
              acc :+ descriptor
          }
        }
      }
      val byteProj = projDes.foldLeft(List.empty[LevelDBByteProjection]) {
        case (l, des) => l :+ byteProjectionInstance2(des).sample.get
      }
      
      projDes.zip(byteProj).map { 
        case (des, byte) =>
          val identities: Vector[Long] = constructIds(des.indexedColumns.values.toSet.size)
          val values: Seq[CValue] = constructValues(des.columns)
          val (projectedIds, projectedValues) = byte.project(VectorCase.fromSeq(identities), values)

          byte.unproject(projectedIds, projectedValues) must_== (identities, values)
      }
    }
  }

  "a byte projection of an empty array" should {
    "return the arguments of project, when unproject is applied to project" in {
      val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable
      val dataPath = Path("/test")
      implicit val (sampleData, _) = DistributedSampleSet.sample(5, 0, AdSamples.emptyArraySample)

      val projDes: List[ProjectionDescriptor] = {
        sampleData.zipWithIndex.foldLeft(List.empty[ProjectionDescriptor]) {
          case (acc, (jobj, i)) => routingTable.route(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
            case (acc, ProjectionData(descriptor, identities, values, _)) =>
              acc :+ descriptor
          }
        }
      }
      val byteProj = projDes.foldLeft(List.empty[LevelDBByteProjection]) {
        case (l, des) => l :+ byteProjectionInstance2(des).sample.get
      }
      
      projDes.zip(byteProj).map { 
        case (des, byte) =>
          val identities: Vector[Long] = constructIds(des.indexedColumns.values.toSet.size)
          val values: Seq[CValue] = constructValues(des.columns)
          val (projectedIds, projectedValues) = byte.project(VectorCase.fromSeq(identities), values)

          byte.unproject(projectedIds, projectedValues) must_== (identities, values)
      }
    }
  }

  "a byte projection of an empty object" should {
    "return the arguments of project, when unproject is applied to project" in {
      val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable
      val dataPath = Path("/test")
      implicit val (sampleData, _) = DistributedSampleSet.sample(5, 0, AdSamples.emptyObjectSample)

      val projDes: List[ProjectionDescriptor] = {
        sampleData.zipWithIndex.foldLeft(List.empty[ProjectionDescriptor]) {
          case (acc, (jobj, i)) => routingTable.route(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
            case (acc, ProjectionData(descriptor, identities, values, _)) =>
              acc :+ descriptor
          }
        }
      }
      val byteProj = projDes.foldLeft(List.empty[LevelDBByteProjection]) {
        case (l, des) => l :+ byteProjectionInstance2(des).sample.get
      }
      
      projDes.zip(byteProj).map { 
        case (des, byte) =>
          val identities: Vector[Long] = constructIds(des.indexedColumns.values.toSet.size)
          val values: Seq[CValue] = constructValues(des.columns)
          val (projectedIds, projectedValues) = byte.project(VectorCase.fromSeq(identities), values)

          byte.unproject(projectedIds, projectedValues) must_== (identities, values)
      }
    }
  }

  "a byte projection of null" should {
    "return the arguments of project, when unproject is applied to project" in {
      val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable
      val dataPath = Path("/test")
      implicit val (sampleData, _) = DistributedSampleSet.sample(5, 0, AdSamples.nullSample)

      val projDes: List[ProjectionDescriptor] = {
        sampleData.zipWithIndex.foldLeft(List.empty[ProjectionDescriptor]) {
          case (acc, (jobj, i)) => routingTable.route(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
            case (acc, ProjectionData(descriptor, identities, values, _)) =>
              acc :+ descriptor
          }
        }
      }
      val byteProj = projDes.foldLeft(List.empty[LevelDBByteProjection]) {
        case (l, des) => l :+ byteProjectionInstance2(des).sample.get
      }
      
      projDes.zip(byteProj).map { 
        case (des, byte) =>
          val identities: Vector[Long] = constructIds(des.indexedColumns.values.toSet.size)
          val values: Seq[CValue] = constructValues(des.columns)
          val (projectedIds, projectedValues) = byte.project(VectorCase.fromSeq(identities), values)

          byte.unproject(projectedIds, projectedValues) must_== (identities, values)
      }
    }
  }

  /* "a scalacheck-generated byte projection" should {
    "return the arguments of project, when unproject is applied to project" in {
          
      check { (byteProjection: LevelDBByteProjection) =>
        val identities: Vector[Long] = constructIds(byteProjection.descriptor.indexedColumns.values.toList.distinct.size)
        val values: Seq[CValue] = constructValues(byteProjection.descriptor.columns)

        val (projectedIds, projectedValues) = byteProjection.project(identities, values)

        byteProjection.unproject(projectedIds, projectedValues) must_== (identities, values) 
      }.set(maxDiscarded -> 500, minTestsOk -> 200)
    }
  } */

  "a byte projection" should {
    "project to the expected key format when a single id matches two sortings, ById and ByValue (test1)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesLong -> 0, colDesBoolean -> 0, colDesFloat -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesLong, ById),(colDesBoolean, ByValue),(colDesFloat, ByValueThenId))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,0,0,0,0,1,1,64,-64,0,0,0,0,0,0,0,0,0,2)
      val expectedValue: Array[Byte] = Array(0,0,0,0,0,0,0,5)

      val (key, value) = byteProjection.project(VectorCase(1L,2L), Seq(cvLong, cvBoolean, cvFloat))
      key must_== expectedKey
      value must_== expectedValue
    }

    "project to the expected key format another when an id is only sorted ByValue (test2)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt -> 0, colDesDouble -> 1, colDesBoolean -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesDouble, ByValue),(colDesBoolean, ByValue))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,4,64,28,0,0,0,0,0,0,1,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,2)
      val expectedValue: Array[Byte] = Array()

      val (key, value) = byteProjection.project(VectorCase(1L,2L), Seq(cvInt, cvDouble, cvBoolean))
      key must_== expectedKey
      value must_== expectedValue
    } 

    "project to the expected key format (case with five columns) (test3)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt -> 0, colDesLong -> 0, colDesFloat -> 1, colDesDouble -> 2, colDesBoolean -> 2)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesLong, ByValue), (colDesFloat, ById), (colDesDouble, ByValue), (colDesBoolean, ById)) 
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,4,0,0,0,0,0,0,0,5,0,0,0,0,0,0,0,2,64,28,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,0,0,1)
      val expectedValue: Array[Byte] = Array(64,-64,0,0,1)

      val (key, value) = byteProjection.project(VectorCase(1L,2L,3L), Seq(cvInt, cvLong, cvFloat, cvDouble, cvBoolean))
      key must_== expectedKey
      value must_== expectedValue
    }
  }

  "when applied to the project function, the unproject function" should {
    "return the arguments of the project function (test1)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesLong -> 0, colDesBoolean -> 0, colDesFloat -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesLong, ById),(colDesBoolean, ByValue),(colDesFloat, ByValueThenId))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val (projectedIds, projectedValues) = byteProjection.project(VectorCase(1L,2L), Seq(cvLong, cvBoolean, cvFloat))

      byteProjection.unproject(projectedIds, projectedValues) must_== (VectorCase(1L,2L), Seq(cvLong, cvBoolean, cvFloat)) 
    }

    "return the arguments of the project function (test2)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt -> 0, colDesDouble -> 1, colDesBoolean -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesDouble, ByValue),(colDesBoolean, ByValue))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val (projectedIds, projectedValues) = byteProjection.project(VectorCase(1L,2L), Seq(cvInt, cvDouble, cvBoolean))

      byteProjection.unproject(projectedIds, projectedValues) must_== (VectorCase(1L,2L), Seq(cvInt, cvDouble, cvBoolean)) 
    }

    "return the arguments of the project function (test3)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesFloat -> 0, colDesInt -> 1, colDesDouble -> 1, colDesBoolean -> 2, colDesDouble2 -> 2)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesFloat, ById),(colDesInt, ByValue), (colDesDouble, ById), (colDesBoolean, ByValue), (colDesDouble2, ById)) 
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }
 
      val (projectedIds, projectedValues) = byteProjection.project(VectorCase(1L,2L,3L), Seq(cvFloat, cvInt, cvDouble, cvBoolean, cvDouble2))

      byteProjection.unproject(projectedIds, projectedValues) must_== (VectorCase(1L,2L,3L), Seq(cvFloat, cvInt, cvDouble, cvBoolean, cvDouble2)) 
    }

    
  }

}







