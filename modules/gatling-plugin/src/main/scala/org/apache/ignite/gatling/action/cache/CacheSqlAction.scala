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
import io.gatling.commons.validation.Validation
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.gatling.Predef.SqlCheck
import org.apache.ignite.gatling.action.CacheAction

/**
 * Action for the SQL query Ignite operation.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param requestName Name of the request.
 * @param cacheName Name of cache.
 * @param sql SQL query.
 * @param args Collection of positional arguments for SQL query (may be empty).
 * @param partitions List of cache partitions to use for query (may be empty).
 * @param keepBinary True if it should operate with binary objects.
 * @param checks Collection of checks to perform against the operation result.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CacheSqlAction[K, V](
  requestName: Expression[String],
  cacheName: Expression[String],
  sql: Expression[String],
  args: Seq[Expression[Any]],
  partitions: Expression[List[Int]],
  keepBinary: Boolean,
  checks: Seq[SqlCheck],
  next: Action,
  ctx: ScenarioContext
) extends CacheAction[K, V]("sql", requestName, ctx, next, cacheName, keepBinary) {

  /**
   * Resolves SQL query arguments using the session context.
   *
   * @param session Session.
   * @return List of the resolved arguments.
   */
  private def resolveArgs(session: Session): Validation[List[Any]] =
    args
      .foldLeft(List[Any]().success) { case (r, v) =>
        r.flatMap(m => v(session).map(rv => rv :: m))
      }
      .map(l => l.reverse)

  /**
   * Method executed when the Action received a Session message.
   * @param session Session
   */
  override protected def execute(session: Session): Unit = withSessionCheck(session) {
    for {
      CacheActionParameters(resolvedRequestName, cacheApi, _) <- resolveCacheParameters(session)
      resolvedSql <- sql(session)
      resolvedArgs <- resolveArgs(session)
      resolvedPartitions <- partitions(session)
    } yield {
      logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

      val query: SqlFieldsQuery = new SqlFieldsQuery(resolvedSql)

      if (resolvedArgs.nonEmpty) {
        query.setArgs(resolvedArgs: _*)
      }
      if (resolvedPartitions.nonEmpty) {
        query.setPartitions(resolvedPartitions: _*)
      }
      query.setSchema("PUBLIC")
      val func = cacheApi.sql(query) _

      call(func, resolvedRequestName, session, checks)
    }
  }
}
