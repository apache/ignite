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
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.SerializeSeparately;

/**
 *
 */
public class CacheConfigurationEnricher {
    /**
     * Enriches cache configuration fields with deserialized values from given @{code enrichment}.
     *
     * @param ccfg Cache configuration to enrich.
     * @param enrichment Cache configuration enrichment.
     * @param affinityNode {@code true} if enrichment is happened on affinity node.
     *
     * @return Enriched cache configuration.
     */
    public static CacheConfiguration<?, ?> enrich(
        CacheConfiguration<?, ?> ccfg,
        CacheConfigurationEnrichment enrichment,
        boolean affinityNode
    ) {
        CacheConfiguration<?, ?> enrichedCp = new CacheConfiguration<>(ccfg);

        try {
            for (Field field : CacheConfiguration.class.getDeclaredFields())
                if (field.getDeclaredAnnotation(SerializeSeparately.class) != null) {
                    if (!affinityNode && skipDeserialization(ccfg, field))
                        continue;

                    field.setAccessible(true);

                    Object enrichedVal = enrichment.getFieldValue(field.getName());

                    field.set(enrichedCp, enrichedVal);
                }

            // Enrich near cache configuration as well.
            if (enrichment.nearCacheConfigurationEnrichment() != null) {
                NearCacheConfiguration nearEnrichedCp = new NearCacheConfiguration(ccfg.getNearConfiguration());

                for (Field field : NearCacheConfiguration.class.getDeclaredFields())
                    if (field.getDeclaredAnnotation(SerializeSeparately.class) != null) {
                        field.setAccessible(true);

                        Object enrichedVal = enrichment.nearCacheConfigurationEnrichment().getFieldValue(field.getName());

                        field.set(nearEnrichedCp, enrichedVal);
                    }

                enrichedCp.setNearConfiguration(nearEnrichedCp);
            }
        }
        catch (Exception e) {
            throw new IgniteException("Failed to enrich cache configuration " + ccfg.getName(), e);
        }

        return enrichedCp;
    }

    /**
     * @see #enrich(CacheConfiguration, CacheConfigurationEnrichment, boolean).
     * Does the same thing but without skipping any fields.
     */
    public static CacheConfiguration<?, ?> enrichFully(
        CacheConfiguration<?, ?> ccfg,
        CacheConfigurationEnrichment enrichment
    ) {
        return enrich(ccfg, enrichment, true);
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
