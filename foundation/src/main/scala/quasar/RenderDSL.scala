/*
 * Copyright 2020 Precog Data
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

package quasar

trait RenderDSL[A] {
  def toDsl(a: A): DSLTree
}


object RenderDSL {
  def apply[A](implicit ev: RenderDSL[A]): RenderDSL[A] = ev

  trait Ops[A] {
    def toDsl: DSLTree
  }

  object ops {
    implicit def toRenderDSLOps[A](target: A)(implicit tc: RenderDSL[A]): Ops[A] = new Ops[A] {
      def toDsl: DSLTree = RenderDSL[A].toDsl(target)
    }
  }
}
