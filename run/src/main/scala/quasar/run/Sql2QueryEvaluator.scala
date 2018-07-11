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

package quasar.run

import slamdata.Predef.{Long, None}
import quasar.RenderTreeT
import quasar.api.QueryEvaluator
import quasar.common.PhaseResultTell
import quasar.compile.queryPlan
import quasar.frontend.logicalplan.{LogicalPlan => LP}
import quasar.qscript.QScriptEducated
import quasar.qsu.LPtoQS
import quasar.run.implicits._
import quasar.sql.parser

import eu.timepit.refined.auto._
import matryoshka._
import scalaz.{Monad, StateT}
import scalaz.syntax.bind._

object Sql2QueryEvaluator {
  def apply[
    T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
    F[_]: Monad: MonadQuasarErr: PhaseResultTell,
    G[_],
    R](
    qScriptEvaluator: QueryEvaluator[F, G, T[QScriptEducated[T, ?]], R])
    : QueryEvaluator[F, G, SqlQuery, R] =
  QueryEvaluator.mapEval(qScriptEvaluator) { eval => squery =>
    sql2ToQScript[T, F](squery) >>= eval
  }

  def sql2ToQScript[
      T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      F[_]: Monad: MonadQuasarErr: PhaseResultTell](
      sqlQuery: SqlQuery)
      : F[T[QScriptEducated[T, ?]]] =
    for {
      sql <- MonadQuasarErr[F].unattempt_(
        parser[T].parseExpr(sqlQuery.query.value).leftMap(QuasarError.parsing(_)))

      lp  <- queryPlan[F, T, T[LP]](sql, sqlQuery.vars, sqlQuery.basePath, 0L, None)

      qs  <- LPtoQS[T].apply[StateT[F, Long, ?]](lp).eval(0)
    } yield qs
}
