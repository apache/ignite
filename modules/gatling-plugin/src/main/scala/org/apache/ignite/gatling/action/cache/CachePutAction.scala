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

case class CachePutAction[K, V](requestName: Expression[String],
                                cacheName: Expression[String],
                                pair: Expression[(K, V)],
                                override val keepBinary: Boolean,
                                next: Action,
                                ctx: ScenarioContext
                               ) extends CacheAction[K, V] {

  override val actionType: String = "put"

  override protected def execute(session: Session): Unit = withSession(session) {
    for {
      (resolvedKey, resolvedValue) <- pair(session)
      CommonParameters(resolvedRequestName, cacheApi, transactionApi) <- cacheParameters(session)
    } yield {
      logger.debug(s"session user id: #${session.userId}, before $name($resolvedRequestName)")

      val call = transactionApi
        .map(_ => cacheApi.put(resolvedKey, resolvedValue) _)
        .getOrElse(cacheApi.putAsync(resolvedKey, resolvedValue) _)

      super.call(call, resolvedRequestName, session)
    }
  }
}
