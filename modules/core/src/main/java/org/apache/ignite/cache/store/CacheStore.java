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

package org.apache.ignite.cache.store;

import java.util.Collection;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

/**
 * API for cache persistent storage for read-through and write-through behavior.
 * Persistent store is configured via {@link org.apache.ignite.configuration.CacheConfiguration#getCacheStoreFactory()}
 * configuration property. If not provided, values will be only kept in cache memory
 * or swap storage without ever being persisted to a persistent storage.
 * <p>
 * {@link CacheStoreAdapter} provides default implementation for bulk operations,
 * such as {@link #loadAll(Iterable)},
 * {@link #writeAll(Collection)}, and {@link #deleteAll(Collection)}
 * by sequentially calling corresponding {@link #load(Object)},
 * {@link #write(javax.cache.Cache.Entry)}, and {@link #delete(Object)}
 * operations. Use this adapter whenever such behaviour is acceptable. However
 * in many cases it maybe more preferable to take advantage of database batch update
 * functionality, and therefore default adapter implementation may not be the best option.
 * <p>
 * Provided implementations may be used for test purposes:
 * <ul>
 *     <li>{@ignitelink org.apache.ignite.cache.store.hibernate.CacheHibernateBlobStore}</li>
 *     <li>{@link CacheJdbcBlobStore}</li>
 * </ul>
 * <p>
 * All transactional operations of this API are provided with ongoing {@link Transaction},
 * if any. You can attach any metadata to it, e.g. to recognize if several operations belong
 * to the same transaction or not.
 * Here is an example of how attach a JDBC connection as transaction metadata:
 * <pre name="code" class="java">
 * Connection conn = tx.meta("some.name");
 *
 * if (conn == null) {
 *     conn = ...; // Get JDBC connection.
 *
 *     // Store connection in transaction metadata, so it can be accessed
 *     // for other operations on the same transaction.
 *     tx.addMeta("some.name", conn);
 * }
 * </pre>
 *
 * @see CacheStoreSession
 */
public interface CacheStore<K, V> extends CacheLoader<K, V>, CacheWriter<K, V> {
    /**
     * Loads all values from underlying persistent storage. Note that keys are not
     * passed, so it is up to implementation to figure out what to load. This method
     * is called whenever {@link org.apache.ignite.IgniteCache#loadCache(IgniteBiPredicate, Object...)}
     * method is invoked which is usually to preload the cache from persistent storage.
     * <p>
     * This method is optional, and cache implementation does not depend on this
     * method to do anything. Default implementation of this method in
     * {@link CacheStoreAdapter} does nothing.
     * <p>
     * For every loaded value method {@link org.apache.ignite.lang.IgniteBiInClosure#apply(Object, Object)}
     * should be called on the passed in closure. The closure will then make sure
     * that the loaded value is stored in cache.
     *
     * @param clo Closure for loaded values.
     * @param args Arguments passes into
     *      {@link org.apache.ignite.IgniteCache#loadCache(IgniteBiPredicate, Object...)} method.
     * @throws CacheLoaderException If loading failed.
     */
    public void loadCache(IgniteBiInClosure<K, V> clo, @Nullable Object... args) throws CacheLoaderException;

    /**
     * Tells store to commit or rollback a transaction depending on the value of the {@code 'commit'}
     * parameter.
     *
     * @param commit {@code True} if transaction should commit, {@code false} for rollback.
     * @throws CacheWriterException If commit or rollback failed. Note that commit failure in some cases
     *      may bring cache transaction into {@link TransactionState#UNKNOWN} which will
     *      consequently cause all transacted entries to be invalidated.
     * @deprecated Use {@link CacheStoreSessionListener} instead (refer to its JavaDoc for details).
     */
    @Deprecated
    public void sessionEnd(boolean commit) throws CacheWriterException;
}