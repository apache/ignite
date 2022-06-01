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
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.action.IgniteAction
import org.apache.ignite.gatling.api.CacheApi
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.builder.ignite.Configuration

case class CreateCacheAction[K, V](requestName: Expression[String],
                                   cacheName: Expression[String],
                                   config: Configuration[K, V],
                                   next: Action,
                                   ctx: ScenarioContext
                                  ) extends IgniteAction with NameGen {

  override val name: String = genName("createCache")

  override protected def execute(session: Session): Unit = withSession(session) {
    for {
      (resolvedRequestName, igniteApi, _) <- igniteParameters(session)
      resolvedCacheName <- cacheName(session)
    } yield {
      logger.debug(s"session user id: #${session.userId}, before $name")

      val func = getOrCreateCache(igniteApi, resolvedCacheName, config)

      call(func, resolvedRequestName, session)
    }
  }

  private def getOrCreateCache(igniteApi: IgniteApi, cacheName: String, config: Configuration[K, V]): (CacheApi[K, V] => Unit, Throwable => Unit) => Unit = {
    if (config == null) {
      igniteApi.getOrCreateCache(cacheName)
    } else if (config.cacheCfg.isDefined) {
      igniteApi.getOrCreateCacheByConfiguration(config.cacheCfg.get)
    } else if (config.clientCacheCfg.isDefined) {
      igniteApi.getOrCreateCacheByClientConfiguration(config.clientCacheCfg.get)
    } else {
      igniteApi.getOrCreateCacheBySimpleConfig(cacheName, config.simpleCfg.get)
    }
  }
}
