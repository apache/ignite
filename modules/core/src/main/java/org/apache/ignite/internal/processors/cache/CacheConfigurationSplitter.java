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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.SerializeSeparately;
import org.apache.ignite.internal.util.typedef.T2;

/**
 * Splits cache configuration into two parts that can be serialized, deserialized separately.
 * This eliminates the need to deserialize part of the configuration and therefore,
 * it does not require user classes on non-affinity nodes.
 */
public interface CacheConfigurationSplitter {
    /**
     * Splits cache configuration associated with the given {@code desc} into two parts
     * {@link CacheConfiguration} and {@link CacheConfigurationEnrichment} that are serialized separately.
     * The fields marked with {@link SerializeSeparately} are placed into {@link CacheConfigurationEnrichment},
     * the corresponding values into {@link CacheConfiguration} are changed with the default ones.
     *
     * @param desc Cache group description to split.
     * @return Split cache configuration.
     * @see SerializeSeparately
     */
    default T2<CacheConfiguration, CacheConfigurationEnrichment> split(CacheGroupDescriptor desc) {
        if (desc.isConfigurationEnriched())
            return split(desc.config());

        return new T2<>(desc.config(), desc.cacheConfigurationEnrichment());
    }

    /**
     * Splits cache configuration associated with the given {@code desc} into two parts
     * {@link CacheConfiguration} and {@link CacheConfigurationEnrichment} that are serialized separately.
     * The fields marked with {@link SerializeSeparately} are placed into {@link CacheConfigurationEnrichment},
     * the corresponding values into {@link CacheConfiguration} are changed with the default ones.
     *
     * @param desc Cache description to split.
     * @return Split cache configuration.
     * @see SerializeSeparately
     */
    default T2<CacheConfiguration, CacheConfigurationEnrichment> split(DynamicCacheDescriptor desc) {
        if (desc.isConfigurationEnriched())
            return split(desc.cacheConfiguration());

        return new T2<>(desc.cacheConfiguration(), desc.cacheConfigurationEnrichment());
    }

    /**
     * Splits the given cache configuration into two parts
     * {@link CacheConfiguration} and {@link CacheConfigurationEnrichment} that are serialized separately.
     * The fields marked with {@link SerializeSeparately} are placed into {@link CacheConfigurationEnrichment},
     * the corresponding values into {@link CacheConfiguration} are changed with the default ones.
     *
     * @param ccfg Cache configuration to split.
     * @return Split cache configuration.
     * @see SerializeSeparately
     */
    T2<CacheConfiguration, CacheConfigurationEnrichment> split(CacheConfiguration ccfg);
}
