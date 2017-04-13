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

package org.apache.ignite.internal.processors.cache.database;

import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Cache data row.
 */
public interface CacheDataRow extends CacheSearchRow {
    /**
     * @return Cache value.
     */
    public CacheObject value();

    /**
     * @return Cache entry version.
     */
    public GridCacheVersion version();

    /**
     * @return Cache id. Stored only if memory policy with configured per-page eviction is used.
     */
    public int cacheId();

    /**
     * @return Expire time.
     */
    public long expireTime();

    /**
     * @return P2p enabled flag.
     */
    public boolean deploymentEnabled();

    /**
     * @return Key class loader ID.
     */
    public IgniteUuid keyClassLoaderId();

    /**
     * @return Value class loader ID.
     */
    public IgniteUuid valueClassLoaderId();

    /**
     * @return Partition for this key.
     */
    public int partition();

    /**
     * @param link Link for this row.
     */
    public void link(long link);

    /**
     * @param key Key.
     */
    public void key(KeyCacheObject key);
}
