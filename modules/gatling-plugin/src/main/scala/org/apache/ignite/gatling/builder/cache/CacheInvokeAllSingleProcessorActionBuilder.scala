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
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.gatling.action.cache.CacheInvokeAllSingleProcessorAction

/**
 * Base invokeAll action builder.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @tparam T Type of the entry processor result.
 * @param cacheName Cache name.
 * @param keys Collection of the cache keys to run entry processor for.
 */
case class CacheInvokeAllSingleProcessorActionBuilderBase[K, V, T](
  cacheName: Expression[String],
  keys: Expression[Set[K]]
) {
  /**
   * Specify the entry processor instance bypassing the additional arguments step.
   *
   * @param entryProcessor Entry processor instance. May be lambda.
   * @return Builder step for common cache action parameters.
   */
  def apply(entryProcessor: CacheEntryProcessor[K, V, T]): CacheInvokeAllSingleProcessorActionBuilder[K, V, T] =
    CacheInvokeAllActionBuilderEntryProcessorStep(cacheName, keys, Seq.empty)(entryProcessor)

  /**
   * Specify additional arguments to pass to the entry processor.
   *
   * @param args Additional arguments to pass to the entry processor.
   * @return Builder step for entry processor.
   */
  def args(args: Expression[Any]*): CacheInvokeAllActionBuilderEntryProcessorStep[K, V, T] =
    CacheInvokeAllActionBuilderEntryProcessorStep[K, V, T](cacheName, keys, args)
}

/**
 * Builder step for entry processor instance specification.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @tparam T Type of the entry processor result.
 * @param cacheName Cache name.
 * @param keys Collection of the cache keys to run entry processor for.
 * @param args Additional arguments to pass to the entry processor.
 */
case class CacheInvokeAllActionBuilderEntryProcessorStep[K, V, T](
  cacheName: Expression[String],
  keys: Expression[Set[K]],
  args: Seq[Expression[Any]]
) {
  /**
   * Specify the entry processor instance.
   *
   * @param entryProcessor Entry processor instance. May be lambda.
   * @return Builder step for common cache action parameters.
   */
  def apply(entryProcessor: CacheEntryProcessor[K, V, T]): CacheInvokeAllSingleProcessorActionBuilder[K, V, T] =
    new CacheInvokeAllSingleProcessorActionBuilder[K, V, T](cacheName, keys, entryProcessor, args)
}

/**
 * Builder step for common cache action parameters.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @tparam T Type of the entry processor result.
 * @param cacheName Cache name.
 * @param keys Collection of the cache keys to run entry processor for.
 * @param args Additional arguments to pass to the entry processor.
 * @param entryProcessor Entry processor instance. May be lambda.
 */
class CacheInvokeAllSingleProcessorActionBuilder[K, V, T](
  cacheName: Expression[String],
  keys: Expression[Set[K]],
  entryProcessor: CacheEntryProcessor[K, V, T],
  args: Seq[Expression[Any]]
) extends ActionBuilder
    with CacheActionCommonParameters
    with CheckParameters[K, EntryProcessorResult[T]] {

  /**
   * Builds an action.
   *
   * @param ctx The scenario context.
   * @param next The action that will be chained with the Action build by this builder.
   * @return The resulting action.
   */
  override def build(ctx: ScenarioContext, next: Action): Action =
    new CacheInvokeAllSingleProcessorAction[K, V, T](
      requestName,
      cacheName,
      keys,
      entryProcessor,
      args,
      keepBinary = withKeepBinary,
      checks,
      next,
      ctx
    )
}
