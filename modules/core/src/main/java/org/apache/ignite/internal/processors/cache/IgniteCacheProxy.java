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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Cache proxy.
 */
public interface IgniteCacheProxy<K, V> extends IgniteCache<K, V>, Externalizable {
    /**
     * @return Context.
     */
    public GridCacheContext<K, V> context();

    /**
     * Gets cache proxy which does not acquire read lock on gateway enter, should be used only if grid read lock is
     * externally acquired.
     *
     * @return Ignite cache proxy with simple gate.
     */
    public IgniteCacheProxy<K, V> cacheNoGate();

    /**
     * Creates projection that will operate with binary objects.
     * <p> Projection returned by this method will force cache not to deserialize binary objects,
     * so keys and values will be returned from cache API methods without changes.
     * Therefore, signature of the projection can contain only following types:
     * <ul>
     *     <li>{@code BinaryObject} for binary classes</li>
     *     <li>All primitives (byte, int, ...) and there boxed versions (Byte, Integer, ...)</li>
     *     <li>Arrays of primitives (byte[], int[], ...)</li>
     *     <li>{@link String} and array of {@link String}s</li>
     *     <li>{@link UUID} and array of {@link UUID}s</li>
     *     <li>{@link Date} and array of {@link Date}s</li>
     *     <li>{@link java.sql.Timestamp} and array of {@link java.sql.Timestamp}s</li>
     *     <li>Enums and array of enums</li>
     *     <li> Maps, collections and array of objects (but objects inside them will still be converted if they are binary) </li>
     * </ul>
     * <p> For example, if you use {@link Integer} as a key and {@code Value} class as a value (which will be
     * stored in binary format), you should acquire following projection to avoid deserialization:
     * <pre>
     * IgniteInternalCache<Integer, GridBinaryObject> prj = cache.keepBinary();
     *
     * // Value is not deserialized and returned in binary format.
     * GridBinaryObject po = prj.get(1);
     * </pre>
     * <p> Note that this method makes sense only if cache is working in binary mode ({@code
     * CacheConfiguration#isBinaryEnabled()} returns {@code true}. If not, this method is no-op and will return
     * current projection.
     *
     * @return Projection for binary objects.
     */
    @SuppressWarnings("unchecked")
    public <K1, V1> IgniteCache<K1, V1> keepBinary();

    /**
     * @param dataCenterId Data center ID.
     * @return Projection for data center id.
     */
    @SuppressWarnings("unchecked")
    public IgniteCache<K, V> withDataCenterId(byte dataCenterId);

    /**
     * @return Cache with skip store enabled.
     */
    public IgniteCache<K, V> skipStore();

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withAllowAtomicOpsInTx();

    /**
     * @return Internal proxy.
     */
    public GridCacheProxyImpl<K, V> internalProxy();

    /**
     * @return {@code True} if proxy was closed.
     */
    public boolean isProxyClosed();

    /**
     * Closes this proxy instance.
     */
    public void closeProxy();

    /**
     * @return Future that contains cache destroy operation.
     */
    public IgniteFuture<?> destroyAsync();

    /**
     * @return Future that contains cache close operation.
     */
    public IgniteFuture<?> closeAsync();

    /**
     * Queries cache with multiple statements. Accepts {@link SqlFieldsQuery} class.
     *
     * @param qry SqlFieldsQuery.
     * @return List of cursors.
     * @see SqlFieldsQuery
     */
    public List<FieldsQueryCursor<List<?>>> queryMultipleStatements(SqlFieldsQuery qry);
}
