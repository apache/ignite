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

import javax.cache.processor.EntryProcessorResult

import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.EmptyStringExpressionSuccess
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.gatling.IgniteCheck
import org.apache.ignite.gatling.action.cache.CacheInvokeAllMapAction
import org.apache.ignite.gatling.action.cache.CacheInvokeAllSingleProcessorAction
import org.apache.ignite.gatling.builder.IgniteActionBuilder

case class CacheInvokeAllSingleProcessorActionBuilderBase[K, V, T](
  cacheName: Expression[String],
  keys: Expression[Set[K]],
  requestName: Expression[String] = EmptyStringExpressionSuccess
) {

  def apply(entryProcessor: CacheEntryProcessor[K, V, T]): CacheInvokeAllSingleProcessorActionBuilder[K, V, T] =
    CacheInvokeAllSingleProcessorActionBuilder[K, V, T](requestName, cacheName, keys, entryProcessor, Seq.empty)

  def args(args: Expression[Any]*): CacheInvokeAllActionBuilderProcessorStep[K, V, T] =
    CacheInvokeAllActionBuilderProcessorStep[K, V, T](requestName, cacheName, keys, args)
}

case class CacheInvokeAllActionBuilderProcessorStep[K, V, T](
  requestName: Expression[String],
  cacheName: Expression[String],
  keys: Expression[Set[K]],
  arguments: Seq[Expression[Any]]
) {

  def apply(entryProcessor: CacheEntryProcessor[K, V, T]): CacheInvokeAllSingleProcessorActionBuilder[K, V, T] =
    CacheInvokeAllSingleProcessorActionBuilder[K, V, T](requestName, cacheName, keys, entryProcessor, arguments)
}

case class CacheInvokeAllSingleProcessorActionBuilder[K, V, T](
  requestName: Expression[String],
  cacheName: Expression[String],
  keys: Expression[Set[K]],
  entryProcessor: CacheEntryProcessor[K, V, T],
  arguments: Seq[Expression[Any]],
  checks: Seq[IgniteCheck[K, EntryProcessorResult[T]]] = Seq.empty
) extends IgniteActionBuilder {

  def check(newChecks: IgniteCheck[K, EntryProcessorResult[T]]*): CacheInvokeAllSingleProcessorActionBuilder[K, V, T] =
    this.copy(checks = newChecks)

  def as(requestName: Expression[String]): ActionBuilder = this.copy(requestName = requestName)

  override def build(ctx: ScenarioContext, next: Action): Action =
    new CacheInvokeAllSingleProcessorAction[K, V, T](
      requestName,
      cacheName,
      keys,
      entryProcessor,
      arguments,
      keepBinary = false,
      checks,
      next,
      ctx
    )
}

case class CacheInvokeAllActionBuilderBase[K, V, T](
  cacheName: Expression[String],
  map: Expression[Map[K, CacheEntryProcessor[K, V, T]]],
  requestName: Expression[String] = EmptyStringExpressionSuccess
) extends IgniteActionBuilder {

  def as(requestName: Expression[String]): ActionBuilder = this.copy(requestName = requestName)

  override def build(ctx: ScenarioContext, next: Action): Action =
    new CacheInvokeAllMapAction[K, V, T](requestName, cacheName, map, Seq.empty, keepBinary = false, Seq.empty, next, ctx)

  def args(args: Expression[Any]*): CacheInvokeAllActionBuilder[K, V, T] =
    CacheInvokeAllActionBuilder[K, V, T](requestName, cacheName, map, args)
}

case class CacheInvokeAllActionBuilder[K, V, T](
  requestName: Expression[String],
  cacheName: Expression[String],
  map: Expression[Map[K, CacheEntryProcessor[K, V, T]]],
  arguments: Seq[Expression[Any]],
  checks: Seq[IgniteCheck[K, EntryProcessorResult[T]]] = Seq.empty
) extends IgniteActionBuilder {

  def check(newChecks: IgniteCheck[K, EntryProcessorResult[T]]*): CacheInvokeAllActionBuilder[K, V, T] = this.copy(checks = newChecks)

  def as(requestName: Expression[String]): ActionBuilder = this.copy(requestName = requestName)

  override def build(ctx: ScenarioContext, next: Action): Action =
    new CacheInvokeAllMapAction[K, V, T](requestName, cacheName, map, arguments, keepBinary = false, checks, next, ctx)
}
