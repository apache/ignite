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
package org.apache.ignite.gatling.builder.cache

import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.EmptyStringExpressionSuccess
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.Predef.SqlCheck
import org.apache.ignite.gatling.action.cache.CacheSqlAction

/**
 * SQL query action builder.
 *
 * @param cacheName Cache name.
 * @param sql SQL query.
 * @param argsList Optional list of the positional query parameters.
 * @param partitionsList Optional list of cache partitions to execute query on.
 * @param checks Optional collection of check to be performed on the query result.
 * @param requestName Request name.
 */
case class CacheSqlActionBuilder(
  cacheName: Expression[String],
  sql: Expression[String],
  argsList: List[Expression[Any]] = List.empty,
  partitionsList: Expression[List[Int]] = _ => List.empty.success,
  checks: Seq[SqlCheck] = Seq.empty,
  requestName: Expression[String] = EmptyStringExpressionSuccess
) extends ActionBuilder {

  /**
   * Specify collection of check to be performed on the query result.
   *
   * @param newChecks collection of check to be performed on the query result.
   * @return itself.
   */
  def check(newChecks: SqlCheck*): CacheSqlActionBuilder = this.copy(checks = newChecks)

  /**
   * Specify list of the positional query parameters.
   *
   * @param newArgs list of the positional query parameters.
   * @return itself.
   */
  def args(newArgs: Expression[Any]*): CacheSqlActionBuilder = this.copy(argsList = newArgs.toList)

  /**
   * Specify list of partitions.
   *
   * @param newPartitions list of cache partitions to execute query on.
   * @return itself.
   */
  def partitions(newPartitions: Expression[List[Int]]): CacheSqlActionBuilder = this.copy(partitionsList = newPartitions)

  /**
   * Specify request name for action.
   *
   * @param requestName Request name.
   * @return itself.
   */
  def as(requestName: Expression[String]): ActionBuilder = this.copy(requestName = requestName)

  /**
   * Builds an action.
   *
   * @param ctx The scenario context.
   * @param next The action that will be chained with the Action build by this builder.
   * @return The resulting action.
   */
  override def build(ctx: ScenarioContext, next: Action): Action =
    new CacheSqlAction(requestName, cacheName, sql, argsList, partitionsList, keepBinary = false, checks, next, ctx)
}
