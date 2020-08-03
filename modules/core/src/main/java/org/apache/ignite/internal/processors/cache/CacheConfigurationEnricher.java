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

import java.lang.reflect.Field;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.SerializeSeparately;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheConfigurationEnricher {
    /** Marshaller. */
    private final Marshaller marshaller;

    /** Class loader. */
    private final ClassLoader clsLdr;

    /**
     * @param marshaller Marshaller.
     * @param clsLdr Class loader.
     */
    public CacheConfigurationEnricher(Marshaller marshaller, ClassLoader clsLdr) {
        this.marshaller = marshaller;
        this.clsLdr = clsLdr;
    }

    /**
     * Enriches descriptor cache configuration with stored enrichment.
     *
     * @param desc Description.
     * @param affinityNode Affinity node.
     */
    public DynamicCacheDescriptor enrich(DynamicCacheDescriptor desc, boolean affinityNode) {
        if (CU.isUtilityCache(desc.cacheName()))
            return desc;

        if (desc.isConfigurationEnriched())
            return desc;

        CacheConfiguration<?, ?> enrichedCfg = enrich(
            desc.cacheConfiguration(), desc.cacheConfigurationEnrichment(), affinityNode);

        desc.cacheConfiguration(enrichedCfg);

        desc.configurationEnriched(true);

        return desc;
    }

    /**
     * Enriches descriptor cache configuration with stored enrichment.
     *
     * @param desc Description.
     * @param affinityNode Affinity node.
     */
    public CacheGroupDescriptor enrich(CacheGroupDescriptor desc, boolean affinityNode) {
        if (CU.isUtilityCache(desc.cacheOrGroupName()))
            return desc;

        if (desc.isConfigurationEnriched())
            return desc;

        CacheConfiguration<?, ?> enrichedCfg = enrich(
            desc.config(), desc.cacheConfigurationEnrichment(), affinityNode);

        desc.config(enrichedCfg);

        desc.configurationEnriched(true);

        return desc;
    }

    /**
     * Enriches cache configuration fields with deserialized values from given @{code enrichment}.
     *
     * @param ccfg Cache configuration to enrich.
     * @param enrichment Cache configuration enrichment.
     * @param affinityNode {@code true} if enrichment is happened on affinity node.
     *
     * @return Enriched cache configuration.
     */
    public CacheConfiguration<?, ?> enrich(
        CacheConfiguration<?, ?> ccfg,
        @Nullable CacheConfigurationEnrichment enrichment,
        boolean affinityNode
    ) {
        if (enrichment == null)
            return ccfg;

        CacheConfiguration<?, ?> enrichedCp = new CacheConfiguration<>(ccfg);

        try {
            for (Field field : CacheConfiguration.class.getDeclaredFields())
                if (field.getDeclaredAnnotation(SerializeSeparately.class) != null) {
                    if (!affinityNode && skipDeserialization(ccfg, field))
                        continue;

                    field.setAccessible(true);

                    Object enrichedVal = deserialize(
                        field.getName(), enrichment.getFieldSerializedValue(field.getName()));

                    field.set(enrichedCp, enrichedVal);
                }
        }
        catch (Exception e) {
            throw new IgniteException("Failed to enrich cache configuration [cacheName=" + ccfg.getName() + "]", e);
        }

        return enrichedCp;
    }

    /**
     * @see #enrich(CacheConfiguration, CacheConfigurationEnrichment, boolean).
     * Does the same thing but without skipping any fields.
     */
    public CacheConfiguration<?, ?> enrichFully(
        CacheConfiguration<?, ?> ccfg,
        CacheConfigurationEnrichment enrichment
    ) {
        return enrich(ccfg, enrichment, true);
    }

    /**
     * @param fieldName Field name.
     */
    private Object deserialize(String fieldName, byte[] serializedVal) {
        try {
            return U.unmarshal(marshaller, serializedVal, clsLdr);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to deserialize field " + fieldName, e);
        }
    }

    /**
     * Skips deserialization for some fields.
     *
     * @param ccfg Cache configuration.
     * @param field Field.
     *
     * @return {@code true} if deserialization for given field should be skipped.
     */
    private static boolean skipDeserialization(CacheConfiguration<?, ?> ccfg, Field field) {
        if ("storeFactory".equals(field.getName()) && ccfg.getAtomicityMode() == CacheAtomicityMode.ATOMIC)
            return true;

        return false;
    }
}
