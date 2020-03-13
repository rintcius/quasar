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

package quasar.sql

import slamdata.Predef._
import quasar.common.CIName

import scalaz.Scalaz._

class StatementSpec extends quasar.Qspec {
  "Statement" >> {
    "pretty print" >> {
      "escape param names that need to escaped" >> {
        val funcDef = FunctionDecl(
          name = CIName("FOO"),
          args = List(CIName("what!?(")),
          body = sqlE"select * from :`what!?(`")
        funcDef.pprintF must_= "CREATE FUNCTION FOO(:`what!?(`)\n  BEGIN\n    (select * from :`what!?(`)\n  END"
      }
    }
  }
}
