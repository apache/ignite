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

package org.apache.ignite.cache.store;

import java.util.Map;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Session for the cache store operations. The main purpose of cache store session
 * is to hold context between multiple store invocations whenever in transaction. For example,
 * if using JDBC, you can store the ongoing database connection in the session {@link #properties()} map.
 * You can then commit this connection in the {@link CacheStore#sessionEnd(boolean)} method.
 * <p>
 * {@code CacheStoreSession} can be injected into an implementation of {@link CacheStore} with
 * {@link CacheStoreSessionResource @CacheStoreSessionResource} annotation.
 *
 * @see CacheStoreSessionResource
 */
public interface CacheStoreSession {
    /**
     * Gets transaction spanning multiple store operations, or {@code null} if
     * there is no transaction.
     *
     * @return Transaction belonging to current session.
     */
    public Transaction transaction();

    /**
     * Returns {@code true} if performing store operation within a transaction,
     * {@code false} otherwise. Analogous to calling {@code transaction() != null}.
     *
     * @return {@code True} if performing store operation within a transaction,
     * {@code false} otherwise.
     */
    public boolean isWithinTransaction();

    /**
     * Attaches the given object to this session.
     * <p>
     * An attached object may later be retrieved via the {@link #attachment()}
     * method. Invoking this method causes any previous attachment to be
     * discarded. To attach additional objects use {@link #properties()} map.
     * <p>
     * The current attachment may be discarded by attaching {@code null}.
     *
     * @param attachment The object to be attached (or {@code null} to discard current attachment).
     * @return Previously attached object, if any.
     */
    @Nullable public <T> T attach(@Nullable Object attachment);

    /**
     * Retrieves the current attachment or {@code null} if there is no attachment.
     *
     * @return Currently attached object, if any.
     */
    @Nullable public <T> T attachment();

    /**
     * Gets current session properties. You can add properties directly to the
     * returned map.
     *
     * @return Current session properties.
     */
    public <K, V> Map<K, V> properties();

    /**
     * Cache name for the current store operation. Note that if the same store
     * is reused between different caches, then the cache name will change between
     * different store operations.
     *
     * @return Cache name for the current store operation.
     */
    public String cacheName();
}