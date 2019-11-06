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

package org.apache.ignite.lang;

/**
 * Allows to enable asynchronous mode on Ignite APIs, e.g.
 * <pre>
 *     IgniteFuture f = cache.getAsync();
 * </pre>
 * instead of old-style async API:
 * <pre>
 *     IgniteCache asyncCache = cache.withAsync();
 *     asyncCache.get(key);
 *     IgniteFuture fut = asyncCache.future();
 * </pre>
 * @deprecated since 2.0. Please use specialized asynchronous methods.
 */
@Deprecated
public interface IgniteAsyncSupport {
    /**
     * Gets instance of this component with asynchronous mode enabled.
     *
     * @return Instance of this component with asynchronous mode enabled.
     *
     * @deprecated since 2.0. Please use new specialized async method
     * e.g.
     * <pre>
     *     IgniteFuture f = cache.getAsync();
     * </pre>
     * instead of old-style async API:
     * <pre>
     *     IgniteCache asyncCache = cache.withAsync();
     *     asyncCache.get(key);
     *     IgniteFuture fut = asyncCache.future();
     * </pre>
     */
    @Deprecated
    public IgniteAsyncSupport withAsync();

    /**
     * @return {@code True} if asynchronous mode is enabled.
     *
     * @deprecated since 2.0. Please use new specialized async method
     * e.g.
     * <pre>
     *     IgniteFuture f = cache.getAsync();
     * </pre>
     * instead of old-style async API:
     * <pre>
     *     IgniteCache asyncCache = cache.withAsync();
     *     asyncCache.get(key);
     *     IgniteFuture fut = asyncCache.future();
     * </pre>
     */
    @Deprecated
    public boolean isAsync();

    /**
     * Gets and resets future for previous asynchronous operation.
     *
     * @return Future for previous asynchronous operation.
     *
     * @deprecated since 2.0. Please use new specialized async method
     * e.g.
     * <pre>
     *     IgniteFuture f = cache.getAsync();
     * </pre>
     * instead of old-style async API:
     * <pre>
     *     IgniteCache asyncCache = cache.withAsync();
     *     asyncCache.get(key);
     *     IgniteFuture fut = asyncCache.future();
     * </pre>
     */
    @Deprecated
    public <R> IgniteFuture<R> future();
}
