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

package org.apache.ignite.cdc;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spi.systemview.view.CacheView;

/**
 * Notification of {@link CdcConsumer} about cache creation/change events.
 *
 * @see CdcConsumer
 * @see Ignite#createCache(String)
 * @see IgniteCache
 * @see CacheConfiguration
 * @see QueryEntity
 */
public interface CdcCacheEvent {
    /**
     * @return Cache ID.
     * @see CacheView#cacheId()
     */
    public int cacheId();

    /**
     * Note, {@link CacheConfiguration#getQueryEntities()} value not changed on table schema change.
     * Current table schema can be obtained by {@link #queryEntities()}.
     *
     * @return Initial cache configuration.
     */
    public CacheConfiguration<?, ?> configuration();

    /**
     * Returns current state of configured {@link QueryEntity}.
     * {@link QueryEntity} can be changed by executing DDL on SQL tables.
     *
     * Note, {@link CacheConfiguration#getQueryEntities()} returns initial definition of {@link QueryEntity}.
     *
     * @return Query entities for cache.
     */
    public Collection<QueryEntity> queryEntities();
}
