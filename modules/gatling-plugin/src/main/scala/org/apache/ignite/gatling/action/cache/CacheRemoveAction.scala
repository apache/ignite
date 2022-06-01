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
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.action.CacheAction

case class CacheRemoveAction[K, V](requestName: Expression[String],
                                   cacheName: Expression[String],
                                   key: Expression[K],
                                   next: Action,
                                   ctx: ScenarioContext
                                  ) extends CacheAction[K, V] with NameGen {

  override val name: String = genName("cacheGet")

  override protected def execute(session: Session): Unit = withSession(session) {
    for {
      CommonParameters(resolvedRequestName, cacheApi, transactionApi) <- cacheParameters(session)
      resolvedKey <- key(session)
    } yield {
      logger.debug(s"session user id: #${session.userId}, before $name")

      val call = transactionApi
        .map(_ => cacheApi.remove(resolvedKey) _)
        .getOrElse(cacheApi.removeAsync(resolvedKey) _)

      super.call(call, resolvedRequestName, session)
    }
  }
}
