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
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.gatling.action.cache.CacheInvokeAction

/**
 * Base invoke action builder.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @tparam T Type of the entry processor result.
 * @param cacheName Cache name.
 * @param key The cache entry key to run entry processor for.
 */
case class CacheInvokeActionBuilderBase[K, V, T](
  cacheName: Expression[String],
  key: Expression[K]
) {
  /**
   * Specify the entry processor instance bypassing the additional arguments step.
   *
   * @param entryProcessor Entry processor instance. May be lambda.
   * @return Builder step for common cache action parameters.
   */
  def apply(entryProcessor: CacheEntryProcessor[K, V, T]): CacheInvokeActionBuilder[K, V, T] =
    CacheInvokeActionBuilderProcessorStep(cacheName, key, Seq.empty)(entryProcessor)

  /**
   * Specify additional arguments to pass to the entry processor.
   *
   * @param args Additional arguments to pass to the entry processor.
   * @return Builder step for entry processor.
   */
  def args(args: Expression[Any]*): CacheInvokeActionBuilderProcessorStep[K, V, T] =
    CacheInvokeActionBuilderProcessorStep[K, V, T](cacheName, key, args)
}

/**
 * Builder step for entry processor instance specification.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @tparam T Type of the entry processor result.
 * @param cacheName Cache name.
 * @param key The cache entry key to run entry processor for.
 * @param args Additional arguments to pass to the entry processor.
 */
case class CacheInvokeActionBuilderProcessorStep[K, V, T](
  cacheName: Expression[String],
  key: Expression[K],
  args: Seq[Expression[Any]]
) {
  /**
   * Specify the entry processor instance.
   *
   * @param entryProcessor Entry processor instance. May be lambda.
   * @return Builder step for common cache action parameters.
   */
  def apply(entryProcessor: CacheEntryProcessor[K, V, T]): CacheInvokeActionBuilder[K, V, T] =
    new CacheInvokeActionBuilder[K, V, T](cacheName, key, entryProcessor, args)
}

/**
 * Builder step for common cache action parameters.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @tparam T Type of the entry processor result.
 * @param cacheName Cache name.
 * @param key The cache entry key.
 * @param entryProcessor Instance of CacheEntryProcessor.
 * @param arguments Additional arguments to pass to the entry processor.
 */
class CacheInvokeActionBuilder[K, V, T](
  cacheName: Expression[String],
  key: Expression[K],
  entryProcessor: CacheEntryProcessor[K, V, T],
  arguments: Seq[Expression[Any]]
) extends ActionBuilder
    with CacheActionCommonParameters
    with CheckParameters[K, T] {
  /**
   * Builds an action.
   *
   * @param ctx The scenario context.
   * @param next The action that will be chained with the Action build by this builder.
   * @return The resulting action.
   */
  override def build(ctx: ScenarioContext, next: Action): Action =
    new CacheInvokeAction[K, V, T](
      requestName,
      cacheName,
      key,
      entryProcessor,
      arguments,
      keepBinary = withKeepBinary,
      checks,
      next,
      ctx
    )
}
