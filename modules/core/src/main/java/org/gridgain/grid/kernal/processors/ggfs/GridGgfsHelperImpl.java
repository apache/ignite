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

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.cache.eviction.ggfs.*;

/**
 * GGFS utils processor.
 */
public class GridGgfsHelperImpl implements GridGgfsHelper {
    /** {@inheritDoc} */
    @Override public void preProcessCacheConfiguration(CacheConfiguration cfg) {
        GridCacheEvictionPolicy evictPlc = cfg.getEvictionPolicy();

        if (evictPlc instanceof GridCacheGgfsPerBlockLruEvictionPolicy && cfg.getEvictionFilter() == null)
            cfg.setEvictionFilter(new GridCacheGgfsEvictionFilter());
    }

    /** {@inheritDoc} */
    @Override public void validateCacheConfiguration(CacheConfiguration cfg) throws IgniteCheckedException {
        GridCacheEvictionPolicy evictPlc =  cfg.getEvictionPolicy();

        if (evictPlc != null && evictPlc instanceof GridCacheGgfsPerBlockLruEvictionPolicy) {
            GridCacheEvictionFilter evictFilter = cfg.getEvictionFilter();

            if (evictFilter != null && !(evictFilter instanceof GridCacheGgfsEvictionFilter))
                throw new IgniteCheckedException("Eviction filter cannot be set explicitly when using " +
                    "GridCacheGgfsPerBlockLruEvictionPolicy:" + cfg.getName());
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isGgfsBlockKey(Object key) {
        return key instanceof GridGgfsBlockKey;
    }
}
