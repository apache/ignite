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

import java.io.Serializable;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * Object that contains serialized values for fields marked with {@link org.apache.ignite.configuration.SerializeSeparately}
 * in {@link org.apache.ignite.configuration.CacheConfiguration}.
 * This object is needed to exchange and store shrinked cache configurations to avoid possible {@link ClassNotFoundException} errors
 * during deserialization on nodes where some specific class may not exist.
 */
public class CacheConfigurationEnrichment implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Field name -> Field serialized value. */
    private final Map<String, byte[]> enrichFields;

    /** Field name -> Field value class name. */
    private final Map<String, String> fieldClassNames;

    /** Enrichment fields for {@link org.apache.ignite.configuration.NearCacheConfiguration}. */
    private volatile @Nullable CacheConfigurationEnrichment nearCacheCfgEnrichment;

    /**
     * @param enrichFields Enrich fields.
     * @param fieldClassNames Field class names.
     */
    public CacheConfigurationEnrichment(
        Map<String, byte[]> enrichFields,
        Map<String, String> fieldClassNames
    ) {
        this.enrichFields = enrichFields;
        this.fieldClassNames = fieldClassNames;
    }

    /**
     * @param fieldName Field name.
     */
    public byte[] getFieldSerializedValue(String fieldName) {
        return enrichFields.get(fieldName);
    }

    /**
     * @param fieldName Field name.
     */
    public String getFieldClassName(String fieldName) {
        return fieldClassNames.get(fieldName);
    }

    /**
     * @param nearCacheCfgEnrichment Enrichment configured for {@link org.apache.ignite.configuration.NearCacheConfiguration}.
     */
    public void nearCacheConfigurationEnrichment(CacheConfigurationEnrichment nearCacheCfgEnrichment) {
        this.nearCacheCfgEnrichment = nearCacheCfgEnrichment;
    }

    /**
     * @return Enrichment for configured {@link org.apache.ignite.configuration.NearCacheConfiguration}.
     */
    public CacheConfigurationEnrichment nearCacheConfigurationEnrichment() {
        return nearCacheCfgEnrichment;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheConfigurationEnrichment{" +
            "enrichFields=" + enrichFields +
            '}';
    }
}
