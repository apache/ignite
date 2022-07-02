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
package org.apache.ignite.gatling

import io.gatling.core.Predef.{group => gatlingGroup}
import io.gatling.core.Predef.exec
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ChainBuilder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.ignite.binary.BinaryObjectBuilder
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.builder.cache.Cache
import org.apache.ignite.gatling.builder.ignite.Ignite
import org.apache.ignite.gatling.builder.transaction.Transactions
import org.apache.ignite.gatling.check.IgniteKeyValueEntriesCheckSupport
import org.apache.ignite.gatling.check.IgniteKeyValueMapResultCheckSupport
import org.apache.ignite.gatling.check.IgniteSqlCheckSupport
import org.apache.ignite.gatling.protocol.IgniteProtocol.IgniteApiSessionKey
import org.apache.ignite.gatling.protocol.IgniteProtocolSupport

/**
 * Ignite Gatling DSL functions.
 */
trait IgniteDsl
    extends IgniteProtocolSupport
    with Ignite
    with Transactions
    with Cache
    with IgniteKeyValueMapResultCheckSupport
    with IgniteKeyValueEntriesCheckSupport
    with IgniteSqlCheckSupport {

  /**
   * Allows the `ignite` structure DSL element to be directly created.
   *
   * `ignite` element allows to execute sequence of one or more comma-separated actions as follows
   *
   * {{{
   *   val fragment = ignite(
   *     create("test-cache"),
   *     put("test-cache", "#{key}", "#{value}"),
   *     get(cache, key = "#{key}"
   *   )
   * }}}
   * @param firstBuilder Chain builder foe the first action.
   * @param chainBuilders Chain builders for subsequent actions.
   * @return Final chain builder.
   */
  def ignite(firstBuilder: ChainBuilder, chainBuilders: ChainBuilder*): ChainBuilder =
    exec(firstBuilder).exec(chainBuilders)

  /**
   * Implicit allowing ActionBuilder to be used in place of ChainBuilder.
   *    ggroup(name)(exec(newActionBuilders))
   *
   * Used in the `ignite` DSL structure element in particular.
   *
   * @param actionBuilder ActionBuilder for single action.
   * @return Chain builder construction chain consisting from this single action.
   */
  implicit def actionBuilder2ChainBuilder(actionBuilder: ActionBuilder): ChainBuilder = new ChainBuilder(List(actionBuilder))

  /**
   * Scenario builder allowing the `ignite` structure DSL element.
   *
   * @param scn Original Gatling scenario builder.
   */
  class IgniteScenarioBuilder(scn: ScenarioBuilder) {
    /**
     * Chains `ignite` structure DSL element.
     *
     * @param chainBuilders Chain builders for action to be executed in context of Ignite.
     * @return itself.
     */
    def ignite(chainBuilders: ChainBuilder*): ScenarioBuilder = scn.exec(chainBuilders.map(b => exec(b)))
  }

  /**
   * Adds `ignite` structure DSL element to original gatling scenario builder allowing the following syntax:
   * {{{
   *   val scn = scenario("Ignite")
   *     .feeder(feeder)
   *     .ignite(
   *       create("test-cache"),
   *       put("test-cache", "#{key}", "#{value}")
   *     )
   * }}}
   * @param scn Original gatling scenario builder.
   * @return Ignite scenario builder.
   */
  implicit def scenarioBuilder2IgniteScenarioBuilder(scn: ScenarioBuilder): IgniteScenarioBuilder = new IgniteScenarioBuilder(scn)

  /**
   * Enhanced `group` DSL structure element accepting comma-separated list of chain builders.
   *
   * @param name Group name.
   * @param newActionBuilders Collection of chain builders.
   * @return Chain builder for the actions that make up a group.
   */
  def group(name: Expression[String])(newActionBuilders: ChainBuilder*): ChainBuilder =
    gatlingGroup(name)(exec(newActionBuilders))

  /**
   * Extension methods for session executing the Ignite simulation.
   * @param session Session instance.
   */
  implicit class SessionEx(session: Session) {
    /**
     * Extracts IgniteApi instance from session.
     *
     * It may be needed to access Ignite functionality not exposed via the DSL. Say request
     * the affinity information for cache.
     *
     * @return IgniteApi instance.
     */
    def igniteApi: IgniteApi = session(IgniteApiSessionKey).as[IgniteApi]

    /**
     * Requests instance of the binary object builder from the underlining IgniteApi stored in session.
     *
     * @param typeName Type name.
     * @return Binary object builder instance.
     */
    def binaryBuilder()(typeName: String): BinaryObjectBuilder = igniteApi.binaryObjectBuilder(typeName)
  }
}
