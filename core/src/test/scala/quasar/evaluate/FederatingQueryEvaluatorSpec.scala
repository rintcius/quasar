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

package quasar.evaluate

import slamdata.Predef._
import quasar.TreeMatchers
import quasar.api._, ResourceError._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.contrib.iota.{copkTraverse, copkEqual}
import quasar.fp.{constEqual, reflNT}
import quasar.qscript._

import matryoshka._
import matryoshka.data.Fix
import pathy.Path._
import scalaz.{Id, IList, IMap, Show, Tree}, Id.Id
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.applicative._
import scalaz.syntax.either._

final class FederatingQueryEvaluatorSpec
    extends ResourceDiscoverySpec[Id, IList]
    with TreeMatchers {

  implicit val showTree: Show[Tree[ResourceName]] =
    Show.shows(_.drawTree)

  val abForest =
    Stream(
      Tree.Node(ResourceName("b"), Stream(
        Tree.Leaf(ResourceName("a")))),
      Tree.Leaf(ResourceName("a")))

  val xyForest =
    Stream(
      Tree.Node(ResourceName("x"), Stream(
        Tree.Leaf(ResourceName("y")))),
      Tree.Node(ResourceName("y"), Stream(
        Tree.Leaf(ResourceName("y")))))

  val abs = MockQueryEvaluator.resourceDiscovery[Id, IList](abForest)

  val xys = MockQueryEvaluator.resourceDiscovery[Id, IList](xyForest)

  val qfed = new QueryFederation[Fix, Id, Int, FederatedQuery[Fix, Int]] {
    def evaluateFederated(q: FederatedQuery[Fix, Int]) = q.right[ReadError]
  }

  val fqe =
    FederatingQueryEvaluator(qfed, IMap(
      ResourceName("abs") -> ((abs, 1)),
      ResourceName("xys") -> ((xys, 2))).point[Id])

  // ResourceDiscoverySpec
  val discovery = fqe
  val nonExistentPath = ResourcePath.root() / ResourceName("non") / ResourceName("existent")
  val run = reflNT[Id]

  "children" >> {
    "returns possible keys for root" >> {
      val progeny =
        IList(
          ResourceName("abs") -> ResourcePathType.resourcePrefix,
          ResourceName("xys") -> ResourcePathType.resourcePrefix)

      fqe.children(ResourcePath.root()) must_= progeny.right
    }

    "returns PNF when no source" >> {
      val dne = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")

      fqe.children(dne) must_= pathNotFound(dne).left
    }

    "returns correctly prefixed PNF from source" >> {
      val abx = ResourcePath.root() / ResourceName("abs") / ResourceName("a") / ResourceName("x")

      fqe.children(abx) must_= pathNotFound(abx).left
    }

    "returns results" >> {
      val xx = ResourcePath.root() / ResourceName("xys") / ResourceName("x")

      val expect = IList(ResourceName("y") -> ResourcePathType.resource)

      fqe.children(xx) must_= expect.right
    }
  }

  "isResource" >> {
    "returns false for root" >> {
      fqe.isResource(ResourcePath.root()) must beFalse
    }

    "returns false when no source" >> {
      val noSource = ResourcePath.root() / ResourceName("baz") / ResourceName("bar")

      fqe.isResource(noSource) must beFalse
    }

    "returns false when not a resource" >> {
      val prefix = ResourcePath.root() / ResourceName("xys") / ResourceName("x")

      fqe.isResource(prefix) must beFalse
    }

    "returns true when a resource" >> {
      val resource = ResourcePath.root() / ResourceName("abs") / ResourceName("a")

      fqe.isResource(resource) must beTrue
    }
  }

  "evaluate" >> {
    val qs = construction.mkDefaults[Fix, QScriptRead[Fix, ?]]

    "returns NAR for root" >> {
      val query =
        qs.fix.Map(
          qs.fix.Read[ADir](rootDir),
          qs.recFunc.MakeMapS("value", qs.recFunc.ProjectKeyS(qs.recFunc.Hole, "value")))

      fqe.evaluate(query).swap.toOption must_= Some(notAResource(ResourcePath.root()))
    }

    "returns PNF when no source" >> {
      val query =
        qs.fix.Map(
          qs.fix.Read[AFile](rootDir </> dir("foo") </> file("bar")),
          qs.recFunc.MakeMapS("value", qs.recFunc.ProjectKeyS(qs.recFunc.Hole, "value")))

      val rp =
        ResourcePath.root() / ResourceName("foo") / ResourceName("bar")

      fqe.evaluate(query).swap.toOption must_= Some(pathNotFound(rp))
    }

    "returns PNF when no source in branch" >> {
      val query =
        qs.fix.Union(
          qs.fix.Unreferenced,
          qs.free.Read[AFile](rootDir </> dir("abs") </> file("a")),
          qs.free.Read[AFile](rootDir </> dir("foo") </> file("bar")))

      val rp =
        ResourcePath.root() / ResourceName("foo") / ResourceName("bar")

      fqe.evaluate(query).swap.toOption must_= Some(pathNotFound(rp))
    }

    "builds federated query when all sources found" >> {
      val absa: AFile =
        rootDir </> dir("abs") </> file("a")

      val xysy: AFile =
        rootDir </> dir("xys") </> file("y")

      val query =
        qs.fix.Filter(
          qs.fix.Union(
            qs.fix.Unreferenced,
            qs.free.Read[AFile](absa),
            qs.free.Read[AFile](xysy)),
          qs.recFunc.Gt(
            qs.recFunc.ProjectKeyS(qs.recFunc.Hole, "ts"),
            qs.recFunc.Now))

      fqe.evaluate(query) map { fq =>
        fq.query must beTreeEqual(query)
        fq.sources(absa) must_= Some(Source(ResourcePath.root() / ResourceName("a"), 1))
        fq.sources(xysy) must_= Some(Source(ResourcePath.root() / ResourceName("y"), 2))
      } getOrElse ko("Unexpected evaluate failure.")
    }

    "converts any directory paths to files" >> {
      val absabD: ADir =
        rootDir </> dir("abs") </> dir("a") </> dir("b")

      val absabF: AFile =
        rootDir </> dir("abs") </> dir("a") </> file("b")

      val xysy: AFile =
        rootDir </> dir("xys") </> file("y")

      val query =
        qs.fix.Filter(
          qs.fix.Union(
            qs.fix.Unreferenced,
            qs.free.Read[ADir](absabD),
            qs.free.Read[AFile](xysy)),
          qs.recFunc.Gt(
            qs.recFunc.ProjectKeyS(qs.recFunc.Hole, "ts"),
            qs.recFunc.Now))

      val expected =
        qs.fix.Filter(
          qs.fix.Union(
            qs.fix.Unreferenced,
            qs.free.Read[AFile](absabF),
            qs.free.Read[AFile](xysy)),
          qs.recFunc.Gt(
            qs.recFunc.ProjectKeyS(qs.recFunc.Hole, "ts"),
            qs.recFunc.Now))

      fqe.evaluate(query) map { fq =>
        fq.query must beTreeEqual(expected)
        fq.sources(absabF) must_= Some(Source(ResourcePath.root() / ResourceName("a") / ResourceName("b"), 1))
      } getOrElse ko("Unexpected evaluate failure.")
    }
  }
}
