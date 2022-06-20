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

package org.apache.ignite.gatling.builder.cache

import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.EmptyStringExpressionSuccess
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.gatling.IgniteCheck
import org.apache.ignite.gatling.action.cache.CacheInvokeAction
import org.apache.ignite.gatling.builder.IgniteActionBuilder


case class CacheInvokeActionBuilderBase[K, V, T](cacheName: Expression[String],
                                                 key: Expression[K],
                                                 requestName: Expression[String] = EmptyStringExpressionSuccess) {

  def apply(entryProcessor: CacheEntryProcessor[K, V, T]): CacheInvokeActionBuilder[K, V, T] =
    CacheInvokeActionBuilder[K, V, T](requestName, cacheName, key, entryProcessor, Seq.empty)

  def args(args: Expression[Any]*): CacheInvokeActionBuilderProcessorStep[K, V, T] =
    CacheInvokeActionBuilderProcessorStep[K, V, T](requestName, cacheName, key, args)
}

case class CacheInvokeActionBuilderProcessorStep[K, V, T](requestName: Expression[String],
                                                          cacheName: Expression[String],
                                                          key: Expression[K],
                                                          arguments: Seq[Expression[Any]]) {

  def apply(entryProcessor: CacheEntryProcessor[K, V, T]): CacheInvokeActionBuilder[K, V, T] =
    CacheInvokeActionBuilder[K, V, T](requestName, cacheName, key, entryProcessor, arguments)
}

case class CacheInvokeActionBuilder[K, V, T](requestName: Expression[String],
                                             cacheName: Expression[String],
                                             key: Expression[K],
                                             entryProcessor: CacheEntryProcessor[K, V, T],
                                             arguments: Seq[Expression[Any]],
                                             checks: Seq[IgniteCheck[K, T]] = Seq.empty) extends IgniteActionBuilder {

  def check(newChecks: IgniteCheck[K, T]*): CacheInvokeActionBuilder[K, V, T] = this.copy(checks = newChecks)

  def as(requestName: Expression[String]): ActionBuilder = this.copy(requestName=requestName)

  override def build(ctx: ScenarioContext, next: Action): Action =
    CacheInvokeAction[K, V, T](requestName, cacheName, key, entryProcessor, arguments, checks, next, ctx)
}
