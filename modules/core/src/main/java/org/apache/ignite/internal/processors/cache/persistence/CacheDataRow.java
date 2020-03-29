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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUpdateVersionAware;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Cache data row.
 */
public interface CacheDataRow extends MvccUpdateVersionAware, CacheSearchRow, Storable {
    /**
     * @return Cache value.
     */
    public CacheObject value();

    /**
     * @return Cache entry version.
     */
    public GridCacheVersion version();

    /**
     * @return Expire time.
     */
    public long expireTime();

    /**
     * @return Partition for this key.
     */
    @Override public int partition();

    /**
     * @param link Link for this row.
     */
    @Override public void link(long link);

    /**
     * @param key Key.
     */
    public void key(KeyCacheObject key);

    /** {@inheritDoc} */
    @Override public default IOVersions<? extends AbstractDataPageIO> ioVersions() {
        return DataPageIO.VERSIONS;
    }
}
