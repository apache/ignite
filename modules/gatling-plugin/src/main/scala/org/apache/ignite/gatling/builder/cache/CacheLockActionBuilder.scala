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

import java.util.concurrent.locks.Lock

import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.EmptyStringExpressionSuccess
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.IgniteCheck
import org.apache.ignite.gatling.action.cache.CacheLockAction

/**
 * Cache entry obtain lock action builder.
 *
 * TODO: Note. Although checks are optional in general
 *
 * @tparam K Type of the cache key.
 * @param cacheName Cache name.
 * @param key The cache entry key.
 * @param checks Optional collection of check to be performed on the query result.
 * @param requestName Request name.
 */
case class CacheLockActionBuilder[K](
  cacheName: Expression[String],
  key: Expression[K],
  checks: Seq[IgniteCheck[K, Lock]] = Seq.empty,
  requestName: Expression[String] = EmptyStringExpressionSuccess
) extends ActionBuilder {

  /**
   * Specify collection of check to be performed on the query result.
   *
   * @param newChecks collection of check to be performed on the query result.
   * @return itself.
   */
  def check(newChecks: IgniteCheck[K, Lock]*): CacheLockActionBuilder[K] = this.copy(checks = newChecks)

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
    new CacheLockAction(requestName, cacheName, key, keepBinary = false, checks, next, ctx)
}
