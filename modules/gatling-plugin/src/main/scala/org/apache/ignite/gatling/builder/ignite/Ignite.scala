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
import org.apache.ignite.gatling.builder.cache.Cache
import org.apache.ignite.gatling.builder.ignite
import org.apache.ignite.gatling.builder.transaction.TransactionSupport

case class Ignite(requestName: Expression[String]) extends TransactionSupport {
    def cache(cacheName: Expression[String]): Cache = new Cache(requestName, cacheName)

    def create[K, V](cacheName: Expression[String]): CreateCacheActionBuilderBase[K, V] = CreateCacheActionBuilderBase(requestName, cacheName)
    def start: StartClientActionBuilder = ignite.StartClientActionBuilder(requestName)
    def close: CloseClientActionBuilder = ignite.CloseClientActionBuilder(requestName)

    implicit def createCacheActionBuilderSimpleConfigStep2CreateCacheActionBuilder[K, V](step: CreateCacheActionBuilderSimpleConfigStep): CreateCacheActionBuilder[K, V] =
        step.createCacheActionBuilder
}
