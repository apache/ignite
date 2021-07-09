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

package org.apache.ignite.gatling.actions

import io.gatling.commons.stats.OK
import io.gatling.commons.util.{Clock, DefaultClock}
import io.gatling.core.Predef.Session
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.stats.StatsEngine
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.Ignite
import org.apache.ignite.gatling.protocol.IgniteProtocol

/** */
case class IgniteRequest(tag: String) {
    def cache(name: String) = IgniteCacheRequest(tag, name)
}

/** */
case class IgniteCacheRequest(tag: String, cache: String) {
    def get(key: Expression[AnyRef]) = CacheRequestActionBuilder(tag, (ignite, ses) => {
        ignite.cache(cache).get(key(ses))
    })

    def put(key: Expression[AnyRef], value: Expression[AnyRef]) = CacheRequestActionBuilder(tag, (ignite, ses) => {
        ignite.cache(cache).put(key(ses), value(ses))
    })
}

/** */
case class CacheRequestActionBuilder(tag: String, clo:(Ignite, Session) => Unit) extends ActionBuilder {
    override def build(ctx: ScenarioContext, next: Action): Action = {
        val statsEngine = ctx.coreComponents.statsEngine
        val protocol = ctx.protocolComponentsRegistry.components(IgniteProtocol.igniteProtocolKey).igniteProtocol

        new CacheRequestAction(tag, protocol.ignite, clo, statsEngine, next)
    }
}

/** */
class CacheRequestAction (
    val tag: String,
    val ignite: Ignite,
    val clo:(Ignite, Session) => Unit,
    val statsEngine: StatsEngine,
    val next: Action)
    extends ExitableAction with NameGen {
    override def clock: Clock = new DefaultClock
    override def name: String = genName(tag)

    override protected def execute(session: Session): Unit = {
        val before = clock.nowMillis

        clo.apply(ignite, session)

        next ! session
        session.markAsSucceeded
        statsEngine.logResponse(session.scenario, session.groups, tag, before, clock.nowMillis, OK, None, None)
    }
}


