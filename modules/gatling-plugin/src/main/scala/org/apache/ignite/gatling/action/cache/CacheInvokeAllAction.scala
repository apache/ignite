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

import javax.cache.processor.EntryProcessorResult

import io.gatling.commons.validation.SuccessWrapper
import io.gatling.commons.validation.Validation
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.gatling.IgniteCheck
import org.apache.ignite.gatling.action.CacheAction
import org.apache.ignite.gatling.api.CacheApi
import org.apache.ignite.gatling.api.TransactionApi

abstract class CacheInvokeAllAction[K, V, T] extends CacheAction[K, V] {
  val arguments: Seq[Expression[Any]]
  val checks: Seq[IgniteCheck[K, EntryProcessorResult[T]]]

  def resolveArgs(session: Session): Validation[List[Any]] =
    arguments
      .foldLeft(List[Any]().success) { case (r, v) =>
        r.flatMap(m => v(session).map(rv => rv :: m))
      }
      .map(l => l.reverse)

  def execute(session: Session,
              resolvedRequestName: String,
              cacheApi: CacheApi[K, V],
              transactionApi: Option[TransactionApi],
              resolvedMap: Map[K, CacheEntryProcessor[K, V, T]],
              resolvedArguments: List[Any]): Unit = {

    logger.debug(s"session user id: #${session.userId}, before $name")

    val call = transactionApi
      .map(_ => cacheApi.invokeAll(resolvedMap, resolvedArguments) _)
      .getOrElse(cacheApi.invokeAllAsync(resolvedMap, resolvedArguments) _)

    callWithCheck(call, resolvedRequestName, session, checks)
  }
}

case class CacheInvokeAllMapAction[K, V, T](requestName: Expression[String],
                                            cacheName: Expression[String],
                                            map: Expression[Map[K, CacheEntryProcessor[K, V, T]]],
                                            arguments: Seq[Expression[Any]],
                                            checks: Seq[IgniteCheck[K, EntryProcessorResult[T]]],
                                            next: Action,
                                            ctx: ScenarioContext)
  extends CacheInvokeAllAction[K, V, T] {

  override val actionType: String = "invokeAll"

  override protected def execute(session: Session): Unit = withSession(session) {
    for {
      CommonParameters(resolvedRequestName, cacheApi, transactionApi) <- cacheParameters(session)
      resolvedMap <- map(session)
      resolvedArguments <- resolveArgs(session)
    } yield
      execute(session, resolvedRequestName, cacheApi, transactionApi, resolvedMap, resolvedArguments)
  }
}

case class CacheInvokeAllSingleProcessorAction[K, V, T](requestName: Expression[String],
                                                        cacheName: Expression[String],
                                                        keys: Expression[Set[K]],
                                                        processor: CacheEntryProcessor[K, V, T],
                                                        arguments: Seq[Expression[Any]],
                                                        checks: Seq[IgniteCheck[K, EntryProcessorResult[T]]],
                                                        next: Action,
                                                        ctx: ScenarioContext)
  extends CacheInvokeAllAction[K, V, T] {

  override val actionType: String = "invokeAll"

  override protected def execute(session: Session): Unit = withSession(session) {
    for {
      CommonParameters(resolvedRequestName, cacheApi, transactionApi) <- cacheParameters(session)
      resolvedKeys <- keys(session)
      resolvedMap <- resolvedKeys.map(k => (k, processor)).toMap.success
      resolvedArguments <- resolveArgs(session)
    } yield
      execute(session, resolvedRequestName, cacheApi, transactionApi, resolvedMap, resolvedArguments)
  }
}
