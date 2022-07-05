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

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.action.CacheAction

/**
 * Action for the removeAll Ignite operation.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param requestName Name of the request.
 * @param cacheName Name of cache.
 * @param keys Collection of cache entry keys.
 * @param keepBinary True if it should operate with binary objects.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CacheRemoveAllAction[K, V](
  requestName: Expression[String],
  cacheName: Expression[String],
  keys: Expression[Set[K]],
  keepBinary: Boolean,
  next: Action,
  ctx: ScenarioContext
) extends CacheAction[K, V]("removeAll", requestName, ctx, next, cacheName, keepBinary) {

  /**
   * Method executed when the Action received a Session message.
   * @param session Session
   */
  override protected def execute(session: Session): Unit = withSessionCheck(session) {
    for {
      resolvedKeys <- keys(session)
      CacheActionParameters(resolvedRequestName, cacheApi, transactionApi) <- resolveCacheParameters(session)
    } yield {
      logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

      val call = transactionApi
        .map(_ => cacheApi.removeAll(resolvedKeys) _)
        .getOrElse(cacheApi.removeAllAsync(resolvedKeys) _)

      super.call(call, resolvedRequestName, session)
    }
  }
}
