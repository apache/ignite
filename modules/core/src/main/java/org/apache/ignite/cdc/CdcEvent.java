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

import java.io.Serializable;
import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.spi.systemview.view.CacheView;
import org.jetbrains.annotations.Nullable;

/**
 * Event of single entry change.
 * Instance presents new value of modified entry.
 *
 * @see CdcMain
 * @see CdcConsumer
 */
public interface CdcEvent extends Serializable {
    /**
     * @return Key for the changed entry.
     */
    public Object key();

    /**
     * @return Value for the changed entry or {@code null} in case of entry removal.
     */
    @Nullable public Object value();

    /**
     * @return {@code True} if event fired on primary node for partition containing this entry.
     * @see <a href="
     * https://ignite.apache.org/docs/latest/configuring-caches/configuring-backups#configuring-partition-backups">
     * Configuring partition backups.</a>
     */
    public boolean primary();

    /**
     * Ignite split dataset into smaller chunks to distribute them across the cluster.
     * {@link CdcConsumer} implementations can use {@link #partition()} to split changes processing
     * in the same way as it done for the cache.
     *
     * @return Partition number.
     * @see Affinity#partition(Object)
     * @see Affinity#partitions()
     * @see <a href="https://ignite.apache.org/docs/latest/data-modeling/data-partitioning">Data partitioning</a>
     * @see <a href="https://ignite.apache.org/docs/latest/data-modeling/affinity-collocation">Affinity collocation</a>
     */
    public int partition();

    /**
     * @return Version of the entry.
     */
    public CacheEntryVersion version();

    /**
     * @return Cache ID.
     * @see org.apache.ignite.internal.util.typedef.internal.CU#cacheId(String)
     * @see CacheView#cacheId()
     */
    public int cacheId();

    /**
     * @return Time when entry will be removed from cache. If {@code 0} then entry will be cached until removed.
     * @see org.apache.ignite.IgniteCache#withExpiryPolicy(ExpiryPolicy)
     * @see org.apache.ignite.configuration.CacheConfiguration#setExpiryPolicyFactory(Factory)
     * @see org.apache.ignite.internal.processors.cache.GridCacheUtils#EXPIRE_TIME_ETERNAL
     */
    public long expireTime();
}
