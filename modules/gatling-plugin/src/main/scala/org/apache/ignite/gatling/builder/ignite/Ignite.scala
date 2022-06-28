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
package org.apache.ignite.gatling.builder.ignite

import io.gatling.core.session.Expression

/**
 * DSL to create Ignite operations.
 */
trait Ignite {
  /**
   * Start constructing of the create cache action with the provided cache name.
   *
   * @tparam K Type of the cache key.
   * @tparam V Type of the cache value.
   * @param cacheName Cache name.
   * @return CreateCacheActionBuilderBase
   */
  def create[K, V](cacheName: Expression[String]): CreateCacheActionBuilderBase[K, V] =
    CreateCacheActionBuilderBase(cacheName)

  /**
   * Start constructing of the start Ignite API action.
   *
   * @return StartClientActionBuilder
   */
  def start: StartClientActionBuilder = StartClientActionBuilder()

  /**
   * Start constructing of the close Ignite API action.
   *
   * @return CloseClientActionBuilder
   */
  def close: CloseClientActionBuilder = CloseClientActionBuilder()
}
