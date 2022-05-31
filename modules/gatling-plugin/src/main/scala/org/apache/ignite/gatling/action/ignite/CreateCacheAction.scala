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

import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.action.ActionBase
import org.apache.ignite.gatling.api.{CacheApi, IgniteApi}
import org.apache.ignite.gatling.builder.ignite.Configuration

import scala.util.Try

case class CreateCacheAction[K, V](requestName: Expression[String],
                                   cacheName: Expression[String],
                                   config: Configuration[K, V],
                                   next: Action,
                                   ctx: ScenarioContext
                                  ) extends ChainableAction with NameGen with ActionBase {

  override val name: String = genName("createCache")

  override protected def execute(session: Session): Unit = {

    val igniteApi: IgniteApi = session("igniteApi").as[IgniteApi]

    (for {
      resolvedRequestName <- requestName(session)
      resolvedCacheName <- cacheName(session)
      startTime <- ctx.coreComponents.clock.nowMillis.success
    } yield {
      Try(getOrCreateCache(igniteApi, resolvedCacheName, config)(
        _ => logAndExecuteNext(session, resolvedRequestName, startTime,
          ctx.coreComponents.clock.nowMillis, OK, next, None, None),
        ex => logAndExecuteNext(session, resolvedRequestName, startTime,
          ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage))
      )).recover(ex => {
        ctx.coreComponents.statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, ex.getMessage)
        logAndExecuteNext(session, resolvedRequestName, startTime,
          ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage))
      })
    })
      .onFailure(ex =>
        requestName(session).map { resolvedRequestName =>
          ctx.coreComponents.statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, ex)
          executeNext(session, next)
        },
      )
  }

  private def getOrCreateCache(igniteApi: IgniteApi, cacheName: String, config: Configuration[K, V]): (CacheApi[K, V] => Unit, Throwable => Unit) => Unit = {
    if (config == null)
      igniteApi.getOrCreateCache(cacheName)
    else if (config.cacheCfg != null)
      igniteApi.getOrCreateCacheByConfiguration(config.cacheCfg)
    else if (config.clientCacheCfg != null)
      igniteApi.getOrCreateCacheByClientConfiguration(config.clientCacheCfg)
    else
      igniteApi.getOrCreateCacheBySimpleConfig(cacheName, config.simpleCfg)
  }
}
