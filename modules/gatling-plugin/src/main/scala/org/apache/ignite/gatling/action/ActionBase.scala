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

import java.util.{HashMap => JHashMap}

import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.stats.KO
import io.gatling.commons.stats.OK
import io.gatling.commons.stats.Status
import io.gatling.commons.validation.Failure
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.commons.validation.Validation
import io.gatling.core.action.Action
import io.gatling.core.action.ChainableAction
import io.gatling.core.check.Check
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.api.CacheApi
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.api.TransactionApi
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.gatling.protocol.IgniteProtocol.Components

trait ActionBase extends ChainableAction {
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
                                   message: Option[String]
                                 ): Unit = {
    ctx.coreComponents.statsEngine.logResponse(
      session.scenario,
      session.groups,
      requestName,
      sent,
      received,
      status,
      responseCode,
      message
    )
    next ! session.logGroupRequestTimings(sent, received)
  }

  protected def executeNext(session: Session, next: Action): Unit = next ! session
}

abstract class ActionBaseNew extends ChainableAction {
  val IGNITE_API_SESSION_KEY = "igniteApi"
  val TRANSACTION_API_SESSION_KEY = "transactionApi"

  val name: String
  val requestName: Expression[String]
  val ctx: ScenarioContext
  val next: Action

  protected val components: Components = ctx.protocolComponentsRegistry.components(IgniteProtocol.igniteProtocolKey)

  protected def logAndExecuteNext(session: Session, requestName: String, sent: Long, received: Long, status: Status,
                                  responseCode: Option[String], message: Option[String]): Unit = {
    ctx.coreComponents.statsEngine.logResponse(
      session.scenario,
      session.groups,
      requestName,
      sent,
      received,
      status,
      responseCode,
      message
    )
    next ! session.logGroupRequestTimings(sent, received)
  }

  protected def withSession(session: Session)(f: => Validation[Unit]): Unit = {
    logger.debug(s"session user id: #${session.userId}, $name")
    f
      .onFailure(ex =>
        requestName(session).map { resolvedRequestName =>
          ctx.coreComponents.statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, ex)
          executeNext(session, next)
        }
      )
  }

  def resolveCommonParameters(session: Session): Validation[String] =
    for {
      resolvedRequestName <- requestName(session)
    } yield resolvedRequestName

  def callWithCheck[R](func: (R => Unit, Throwable => Unit) => Unit,
                       resolvedRequestName: String,
                       session: Session,
                       checks: Seq[Check[R]]): Unit = {
    val startTime = ctx.coreComponents.clock.nowMillis
    func(
      value => {
        logger.debug(s"session user id: #${session.userId}, after $name")
        val finishTime = ctx.coreComponents.clock.nowMillis
        val (newSession, error) = Check.check(value, session, checks.toList, new JHashMap[Any, Any]())
        error match {
          case Some(Failure(errorMessage)) =>
            logAndExecuteNext(newSession.markAsFailed, resolvedRequestName, startTime, finishTime, KO, Some("Check ERROR"), Some(errorMessage))
          case _ => logAndExecuteNext(newSession, resolvedRequestName, startTime, finishTime, OK, None, None)
        }
      },
      ex => logAndExecuteNext(session, resolvedRequestName, startTime, ctx.coreComponents.clock.nowMillis, KO, Some("ERROR"), Some(ex.getMessage))
    )
  }

  def call[R](func: (R => Unit, Throwable => Unit) => Unit,
              resolvedRequestName: String,
              session: Session,
              newSession: (Session, Option[R]) => Session = (s: Session, _: Option[R]) => s): Unit = {
    val startTime = ctx.coreComponents.clock.nowMillis
    func(
      r => {
        logger.debug(s"session user id: #${session.userId}, after $name")
        val finishTime = ctx.coreComponents.clock.nowMillis
        logAndExecuteNext(newSession(session, Some(r)), resolvedRequestName, startTime, finishTime, OK, None, None)
      },
      ex => logAndExecuteNext(newSession(session, None), resolvedRequestName, startTime,
        ctx.coreComponents.clock.nowMillis, KO, Some("ERROR"), Some(ex.getMessage))
    )
  }

  protected def executeNext(session: Session, next: Action): Unit = next ! session
}

abstract class CacheAction[K, V] extends IgniteAction with StrictLogging {
  val cacheName: Expression[String]

  case class CommonParameters(requestName: String,
                              cacheApi: CacheApi[K, V],
                              transactionApi: Option[TransactionApi])

  def cacheParameters(session: Session): Validation[CommonParameters] =
    resolveCacheParameters(session)

  def resolveCacheParameters(session: Session): Validation[CommonParameters] =
    for {
      (resolvedRequestName, client, transactionApi) <- resolveIgniteParameters(session)
      resolvedCacheName <- cacheName(session)
      cache <- client.cacheV[K, V](resolvedCacheName)
    } yield CommonParameters(resolvedRequestName, cache, transactionApi)
}

abstract class IgniteAction extends ActionBaseNew with StrictLogging {

  def igniteParameters(session: Session): Validation[(String, IgniteApi, Option[TransactionApi])] =
    resolveIgniteParameters(session)

  def resolveIgniteParameters(session: Session): Validation[(String, IgniteApi, Option[TransactionApi])] =
    for {
      resolvedRequestName <- resolveCommonParameters(session)
      igniteApi <- session(IGNITE_API_SESSION_KEY).validate[IgniteApi]
      transactionApi <- session(TRANSACTION_API_SESSION_KEY).asOption[TransactionApi].success

    } yield (resolvedRequestName, igniteApi, transactionApi)
}
