/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;

/**
 * Ignite thin client.
 * <p>
 * Unlike Ignite client nodes, thin clients do not start Ignite infrastructure and communicate with Ignite cluster
 * over a fast and lightweight protocol.
 * </p>
 */
public interface IgniteClient extends AutoCloseable {
    /**
     * Get existing cache or create the cache if it does not exist.
     *
     * @param name Cache name.
     */
    public <K, V> ClientCache<K, V> getOrCreateCache(String name) throws ClientException;

    /**
     * Get existing cache or create the cache if it does not exist.
     *
     * @param cfg Cache configuration.
     */
    public <K, V> ClientCache<K, V> getOrCreateCache(ClientCacheConfiguration cfg) throws ClientException;

    /**
     * Get existing cache.
     *
     * @param name Cache name.
     */
    public <K, V> ClientCache<K, V> cache(String name);

    /**
     * @return Collection of names of currently available caches or an empty collection if no caches are available.
     */
    public Collection<String> cacheNames() throws ClientException;

    /**
     * Destroy cache.
     */
    public void destroyCache(String name) throws ClientException;

    /**
     * Create cache.
     *
     * @param name Cache name.
     */
    public <K, V> ClientCache<K, V> createCache(String name) throws ClientException;

    /**
     * Create cache.
     *
     * @param cfg Cache configuration.
     */
    public <K, V> ClientCache<K, V> createCache(ClientCacheConfiguration cfg) throws ClientException;

    /**
     * @return Instance of {@link IgniteBinary} interface.
     */
    public IgniteBinary binary();

    /**
     * Execute SQL query and get cursor to iterate over results.
     *
     * @param qry SQL query.
     * @return Cursor.
     */
    public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry);
}
