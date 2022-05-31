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

import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.validation.{Success, SuccessWrapper}
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.action.ActionBase
import org.apache.ignite.gatling.api.{IgniteApi, TransactionApi}
import org.apache.ignite.gatling.builder.transaction.TransactionParameters
import org.apache.ignite.transactions.{TransactionConcurrency, TransactionIsolation}

case class TransactionStartAction(requestName: Expression[String],
                                  params: TransactionParameters,
                                  next: Action,
                                  ctx: ScenarioContext
                                 ) extends ChainableAction with NameGen with ActionBase with StrictLogging {

  override val name: String = genName("txStart")

  override protected def execute(session: Session): Unit = {
    logger.debug(s"session user id: #${session.userId}, txStart")

    val igniteApi: IgniteApi = session("igniteApi").as[IgniteApi]

    (for {
      resolvedRequestName <- requestName(session)
      resolvedTimeout <- params.timeout.map(e => e(session).map(l => Some(l))).getOrElse(Success(Option.empty[Long]))
      resolvedTxSize <- params.txSize.map(e => e(session).map(l => Some(l))).getOrElse(Success(Option.empty[Int]))
      startTime <- ctx.coreComponents.clock.nowMillis.success
    } yield txStart(igniteApi, params.concurrency, params.isolation, resolvedTimeout, resolvedTxSize)(
      transactionApi => logAndExecuteNext(session.set("transactionApi", transactionApi), resolvedRequestName, startTime,
        ctx.coreComponents.clock.nowMillis, OK, next, None, None),
      ex => {
        logger.error(s"session user id: #${session.userId}, txStart failed", ex)
        logAndExecuteNext(session, resolvedRequestName, startTime,
          ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage))
      }
    ))
      .onFailure(ex =>
        requestName(session).map { resolvedRequestName =>
          ctx.coreComponents.statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, ex)
          executeNext(session, next)
        },
      )
  }

  private def txStart(igniteApi: IgniteApi,
                      concurrency: Option[TransactionConcurrency],
                      isolation: Option[TransactionIsolation],
                      timeout: Option[Long],
                      txSize: Option[Int]): (TransactionApi => Unit, Throwable => Unit) => Unit = {
    if (isolation.isDefined && concurrency.isDefined)
      if (timeout.isDefined)
        igniteApi.txStartEx2(params.concurrency.get, params.isolation.get, timeout.get, txSize.getOrElse(0))
      else
        igniteApi.txStartEx(params.concurrency.get, params.isolation.get)
    else
      igniteApi.txStart()
  }
}
