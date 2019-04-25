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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.T2;

/**
 *
 */
public interface CacheConfigurationSplitter {
    /**
     *
     * @param desc Description.
     */
    default T2<CacheConfiguration, CacheConfigurationEnrichment> split(CacheGroupDescriptor desc) {
        if (desc.isConfigurationEnriched())
            return split(desc.config());

        return new T2<>(desc.config(), desc.cacheConfigurationEnrichment());
    }

    /**
     * @param desc Description.
     */
    default T2<CacheConfiguration, CacheConfigurationEnrichment> split(DynamicCacheDescriptor desc) {
        if (desc.isConfigurationEnriched())
            return split(desc.cacheConfiguration());

        return new T2<>(desc.cacheConfiguration(), desc.cacheConfigurationEnrichment());
    }

    /**
     * @param ccfg Cache configuration.
     */
    T2<CacheConfiguration, CacheConfigurationEnrichment> split(CacheConfiguration ccfg);
}
