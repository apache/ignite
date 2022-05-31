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
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.validation.{Failure, SuccessWrapper}
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.check.Check
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.IgniteCheck
import org.apache.ignite.gatling.action.ActionBase
import org.apache.ignite.gatling.api.IgniteApi

import java.util.concurrent.locks.Lock
import java.util.{HashMap => JHashMap}

case class CacheLockAction[K, V](requestName: Expression[String],
                                 cacheName: Expression[String],
                                 key: Expression[K],
                                 checks: Seq[IgniteCheck[K, Lock]],
                                 next: Action,
                                 ctx: ScenarioContext
                               ) extends ChainableAction with NameGen with ActionBase with StrictLogging {

  override val name: String = genName("cacheLock")

  override protected def execute(session: Session): Unit = {
    logger.debug(s"session user id: #${session.userId}, lock")

    val client: IgniteApi = session("igniteApi").as[IgniteApi]

    (for {
      resolvedRequestName <- requestName(session)//                logAndExecuteNext(session.set("lock", value), resolvedRequestName, startTime,
//                  finishTime, OK, next, None, None)

      resolvedCacheName <- cacheName(session)
      resolvedKey <- key(session)
      startTime <- ctx.coreComponents.clock.nowMillis.success
    } yield {
      client.cache[K, V](resolvedCacheName)
        .map(
          cache => {
            logger.debug(s"session user id: #${session.userId}, before cache.lock")

            val lockCall = cache.lock(resolvedKey) _

            lockCall(
              value => {
                logger.debug(s"session user id: #${session.userId}, after cache.lock")
                val finishTime          = ctx.coreComponents.clock.nowMillis

                val (newSession, error) = Check.check(Map(resolvedKey -> value), session, checks.toList,
                  new JHashMap[Any, Any]())
                error match {
                  case Some(Failure(errorMessage)) =>
                    logAndExecuteNext(newSession.markAsFailed, resolvedRequestName, startTime,
                      finishTime, KO, next, Some("Check ERROR"), Some(errorMessage))
                  case _ => logAndExecuteNext(newSession, resolvedRequestName, startTime,
                    finishTime, OK, next, None, None)
                }
              },
              ex => logAndExecuteNext(session, resolvedRequestName, startTime,
                ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage)),
            )
          })
        .fold(
          ex => {
            logger.debug(s"session user id: #${session.userId}, can not get cache in lock", ex)
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
        },
      )
  }
}
