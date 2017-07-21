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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.eviction.EvictionFilter;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.igfs.IgfsEvictionFilter;
import org.apache.ignite.cache.eviction.igfs.IgfsPerBlockLruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * IGFS utils processor.
 */
public class IgfsHelperImpl implements IgfsHelper {
    /** {@inheritDoc} */
    @Override public void preProcessCacheConfiguration(CacheConfiguration cfg) {
        EvictionPolicy evictPlc = cfg.getEvictionPolicy();

        if (evictPlc instanceof IgfsPerBlockLruEvictionPolicy && cfg.getEvictionFilter() == null)
            cfg.setEvictionFilter(new IgfsEvictionFilter());
    }

    /** {@inheritDoc} */
    @Override public void validateCacheConfiguration(CacheConfiguration cfg) throws IgniteCheckedException {
        EvictionPolicy evictPlc =  cfg.getEvictionPolicy();

        if (evictPlc != null && evictPlc instanceof IgfsPerBlockLruEvictionPolicy) {
            EvictionFilter evictFilter = cfg.getEvictionFilter();

            if (evictFilter != null && !(evictFilter instanceof IgfsEvictionFilter))
                throw new IgniteCheckedException("Eviction filter cannot be set explicitly when using " +
                    "IgfsPerBlockLruEvictionPolicy: " + U.maskName(cfg.getName()));
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isIgfsBlockKey(Object key) {
        return key instanceof IgfsBlockKey;
    }
}