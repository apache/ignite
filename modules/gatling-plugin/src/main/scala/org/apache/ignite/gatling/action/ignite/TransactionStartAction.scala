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

import io.gatling.commons.validation.Success
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.action.IgniteAction
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.api.TransactionApi
import org.apache.ignite.gatling.builder.transaction.TransactionParameters
import org.apache.ignite.gatling.protocol.IgniteProtocol.TransactionApiSessionKey
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation

/**
 * Action for the transaction rollback Ignite operation.
 *
 * @param requestName Name of the request.
 * @param params Transaction parameters.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class TransactionStartAction(requestName: Expression[String], params: TransactionParameters, next: Action, ctx: ScenarioContext)
    extends IgniteAction("txStart", requestName, ctx, next) {

  /**
   * Method executed when the Action received a Session message.
   *
   * @param session Session
   */
  override protected def execute(session: Session): Unit = withSessionCheck(session) {
    for {
      IgniteActionParameters(resolvedRequestName, igniteApi, _) <- resolveIgniteParameters(session)
      resolvedTimeout <- params.timeout.map(e => e(session).map(l => Some(l))).getOrElse(Success(Option.empty[Long]))
      resolvedTxSize <- params.txSize.map(e => e(session).map(l => Some(l))).getOrElse(Success(Option.empty[Int]))
    } yield {
      logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

      val func = txStart(igniteApi, params.concurrency, params.isolation, resolvedTimeout, resolvedTxSize)

      call(
        func,
        resolvedRequestName,
        session,
        updateSession = (session, transactionApi: Option[TransactionApi]) =>
          transactionApi
            .map(transactionApi => session.set(TransactionApiSessionKey, transactionApi))
            .getOrElse(session)
      )
    }
  }

  private def txStart(
    igniteApi: IgniteApi,
    concurrency: Option[TransactionConcurrency],
    isolation: Option[TransactionIsolation],
    timeout: Option[Long],
    txSize: Option[Int]
  ): (TransactionApi => Unit, Throwable => Unit) => Unit =
    if (isolation.isDefined && concurrency.isDefined) {
      if (timeout.isDefined) {
        igniteApi.txStart(params.concurrency.get, params.isolation.get, timeout.get, txSize.getOrElse(0))
      } else {
        igniteApi.txStart(params.concurrency.get, params.isolation.get)
      }
    } else {
      igniteApi.txStart()
    }
}
