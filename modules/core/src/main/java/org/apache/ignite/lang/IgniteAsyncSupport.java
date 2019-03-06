/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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