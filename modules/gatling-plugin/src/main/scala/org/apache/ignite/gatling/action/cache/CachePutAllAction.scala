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

import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.stats.KO
import io.gatling.commons.stats.OK
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.action.Action
import io.gatling.core.action.ChainableAction
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.action.ActionBase
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.api.TransactionApi

case class CachePutAllAction[K, V](requestName: Expression[String],
                                   cacheName: Expression[String],
                                   map: Expression[Map[K, V]],
                                   next: Action,
                                   ctx: ScenarioContext
                                  ) extends ChainableAction with NameGen with ActionBase with StrictLogging {

  override val name: String = genName("cachePut")

  override protected def execute(session: Session): Unit = {
    logger.debug(s"session user id: #${session.userId}, put")
    val client: IgniteApi = session("igniteApi").as[IgniteApi]

    (for {
      resolvedRequestName <- requestName(session)
      resolvedCacheName <- cacheName(session)
      resolvedMap <- map(session)
      startTime <- ctx.coreComponents.clock.nowMillis.success
    } yield {
      client.cache[K, V](resolvedCacheName)
        .map(
          cache => {
            logger.debug(s"session user id: #${session.userId}, before cache.putAll")

            val putAllCall = session("transactionApi").asOption[TransactionApi]
              .map(_ => cache.putAll(resolvedMap) _)
              .getOrElse(cache.putAllAsync(resolvedMap) _)

            putAllCall(
              _ => {
                logger.debug(s"session user id: #${session.userId}, after cache.putAll")
                logAndExecuteNext(session, resolvedRequestName, startTime,
                  ctx.coreComponents.clock.nowMillis, OK, next, None, None)
              },
              ex => logAndExecuteNext(session, resolvedRequestName, startTime,
                ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage))
            )
          })
        .fold(
          ex => {
            logger.debug(s"session user id: #${session.userId}, can not get cache in putAll", ex)
            logAndExecuteNext(session, resolvedRequestName, startTime,
              ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage))
          },
          _ => {}
        )
    })
      .onFailure(ex =>
        requestName(session).map { resolvedRequestName =>
          ctx.coreComponents.statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, ex)
          executeNext(session, next)
        }
      )
  }
}
