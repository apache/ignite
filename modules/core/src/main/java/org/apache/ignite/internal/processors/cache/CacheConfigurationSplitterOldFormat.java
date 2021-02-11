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
import org.apache.ignite.internal.util.typedef.T2;

/**
 * Splitter implementation that actually does not split cache configuration.
 * This splitter is needed for backward compatibility.
 */
public class CacheConfigurationSplitterOldFormat implements CacheConfigurationSplitter {
    /** Enricher to merge cache configuration and enrichment to support old (full) format. */
    private final CacheConfigurationEnricher enricher;

    /**
     * Creates a new instance of splitter with the given {@code enricher}.
     *
     * @param enricher Configuration enricher.
     */
    public CacheConfigurationSplitterOldFormat(CacheConfigurationEnricher enricher) {
        this.enricher = enricher;
    }

    /** {@inheritDoc} */
    @Override public T2<CacheConfiguration, CacheConfigurationEnrichment> split(CacheGroupDescriptor desc) {
        if (!desc.isConfigurationEnriched())
            return new T2<>(
                enricher.enrichFully(desc.config(), desc.cacheConfigurationEnrichment()), null);
        else
            return new T2<>(desc.config(), null);
    }

    /** {@inheritDoc} */
    @Override public T2<CacheConfiguration, CacheConfigurationEnrichment> split(DynamicCacheDescriptor desc) {
        if (!desc.isConfigurationEnriched())
            return new T2<>(
                enricher.enrichFully(desc.cacheConfiguration(), desc.cacheConfigurationEnrichment()), null);
        else
            return new T2<>(desc.cacheConfiguration(), null);

    }

    /** {@inheritDoc} */
    @Override public T2<CacheConfiguration, CacheConfigurationEnrichment> split(CacheConfiguration ccfg) {
        return new T2<>(ccfg, null);
    }
}
