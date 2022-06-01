/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.gatling.action.cache

import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.gatling.SqlCheck
import org.apache.ignite.gatling.action.CacheAction

case class CacheSqlAction[K, V](requestName: Expression[String],
                                cacheName: Expression[String],
                                sql: Expression[String],
                                args: List[Expression[Any]],
                                partitions: Expression[List[Int]],
                                checks: Seq[SqlCheck],
                                next: Action,
                                ctx: ScenarioContext
                               ) extends CacheAction[K, V] with NameGen {

  override val name: String = genName("sql")

  private def resolveArgs(session: Session) =
    args
      .foldLeft(List[Any]().success) { case (r, v) =>
        r.flatMap(m => v(session).map(rv => rv :: m))
      }
      .map(l => l.reverse)

  override protected def execute(session: Session): Unit = withSession(session) {
    for {
      CommonParameters(resolvedRequestName, cacheApi, _) <- cacheParameters(session)
      resolvedSql <- sql(session)
      resolvedArgs <- resolveArgs(session)
      resolvedPartitions <- partitions(session)
    } yield {
      logger.debug(s"session user id: #${session.userId}, before $name")

      val query: SqlFieldsQuery = new SqlFieldsQuery(resolvedSql)

      if (resolvedArgs.nonEmpty) {
        query.setArgs(resolvedArgs: _*)
      }
      if (resolvedPartitions.nonEmpty) {
        query.setPartitions(resolvedPartitions: _*)
      }
      query.setSchema("PUBLIC")
      val call = cacheApi.sql(query) _

      callWithCheck(call, resolvedRequestName, session, checks)
    }
  }
}
