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

package org.apache.ignite.gatling.action

import io.gatling.commons.stats.Status
import io.gatling.core.action.Action
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.gatling.protocol.IgniteProtocol.Components

trait ActionBase {
    val ctx: ScenarioContext
    protected val components: Components = ctx.protocolComponentsRegistry.components(IgniteProtocol.igniteProtocolKey)

    protected def logAndExecuteNext(
                               session: Session,
                               requestName: String,
                               sent: Long,
                               received: Long,
                               status: Status,
                               next: Action,
                               responseCode: Option[String],
                               message: Option[String],
                             ): Unit = {
        ctx.coreComponents.statsEngine.logResponse(
            session.scenario,
            session.groups,
            requestName,
            sent,
            received,
            status,
            responseCode,
            message,
        )
        next ! session.logGroupRequestTimings(sent, received)
    }

    protected def executeNext(session: Session, next: Action): Unit = next ! session
}

trait IgniteActionBase {}
