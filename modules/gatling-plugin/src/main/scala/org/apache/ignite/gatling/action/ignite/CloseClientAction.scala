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
package org.apache.ignite.gatling.action.ignite

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.action.IgniteAction
import org.apache.ignite.gatling.protocol.IgniteClientConfigurationCfg
import org.apache.ignite.gatling.protocol.IgniteConfigurationCfg
import org.apache.ignite.gatling.protocol.IgniteProtocol.IgniteApiSessionKey

/**
 * Action for the close Ignite API operation (either closes the thin client or stops the node working in client mode).
 *
 * @param requestName Name of the request.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CloseClientAction(requestName: Expression[String], next: Action, ctx: ScenarioContext)
    extends IgniteAction("close", requestName, ctx, next) {

  /**
   * Method executed when the Action received a Session message.
   * @param session Session
   */
  override protected def execute(session: Session): Unit = withSessionCheck(session) {
    val noOp: (Unit => Unit, Throwable => Unit) => Unit = (s, _) => s.apply(())

    for {
      IgniteActionParameters(resolvedRequestName, igniteApi, _) <- resolveIgniteParameters(session)
    } yield {
      logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

      val func = protocol.cfg match {
        case IgniteClientConfigurationCfg(_) | IgniteConfigurationCfg(_) if protocol.manualClientStart =>
          igniteApi.close() _
        case _ => noOp
      }

      call(func, resolvedRequestName, session, updateSession = (session, _: Option[Unit]) => session.remove(IgniteApiSessionKey))
    }
  }
}
