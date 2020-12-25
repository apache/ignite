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
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.SerializeSeparately;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;

/**
 * Splits cache configuration into two parts that can be serialized, deserialized separately.
 * This eliminates the need to deserialize part of the configuration and therefore,
 * it does not require user classes on non-affinity nodes.
 */
public class CacheConfigurationSplitterImpl implements CacheConfigurationSplitter {
    /** Cache configuration to hold default field values. */
    private static final CacheConfiguration<?, ?> DEFAULT_CACHE_CONFIG = new CacheConfiguration<>();

    /** Near cache configuration to hold default field values. */
    private static final NearCacheConfiguration<?, ?> DEFAULT_NEAR_CACHE_CONFIG = new NearCacheConfiguration<>();

    /** Fields set for V1 splitter protocol. */
    private static final String[] FIELDS_V1 = {"evictPlcFactory", "evictFilter", "storeFactory", "storeSesLsnrs"};

    /** Marshaller. */
    private final Marshaller marshaller;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /**
     * Creates a new instance of splitter.
     *
     * @param ctx Kernal context.
     * @param marshaller Marshaller to be used for seserialization.
     */
    public CacheConfigurationSplitterImpl(GridKernalContext ctx, Marshaller marshaller) {
        this.marshaller = marshaller;
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public T2<CacheConfiguration, CacheConfigurationEnrichment> split(CacheConfiguration ccfg) {
        try {
            CacheConfiguration cfgCp = new CacheConfiguration(ccfg);

            CacheConfigurationEnrichment enrichment = buildEnrichment(
                CacheConfiguration.class, cfgCp, DEFAULT_CACHE_CONFIG);

            return new T2<>(cfgCp, enrichment);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to split cache configuration", e);
        }
    }

    /**
     * Builds {@link CacheConfigurationEnrichment} from given config.
     * It extracts all field values to enrichment object replacing values of that fields with default.
     *
     * @param cfgCls Configuration class.
     * @param cfg Configuration to build enrichment from.
     * @param dfltCfg Default configuration to replace enriched values with default.
     * @param <T> Configuration class.
     * @return Enrichment object for given config.
     * @throws IllegalAccessException If failed.
     */
    private <T> CacheConfigurationEnrichment buildEnrichment(
        Class<T> cfgCls,
        T cfg,
        T dfltCfg
    ) throws IllegalAccessException {
        Map<String, byte[]> enrichment = new HashMap<>();
        Map<String, String> fieldClsNames = new HashMap<>();

        for (Field field : cfgCls.getDeclaredFields()) {
            if (supports(field)) {
                field.setAccessible(true);

                Object val = field.get(cfg);

                byte[] serializedVal = serialize(field.getName(), val);

                enrichment.put(field.getName(), serializedVal);

                fieldClsNames.put(field.getName(), val != null ? val.getClass().getName() : null);

                // Replace field in original configuration with default value.
                field.set(cfg, field.get(dfltCfg));
            }
        }

        return new CacheConfigurationEnrichment(enrichment, fieldClsNames);
    }

    /**
     *  Returns {@code true} if this splitter supports the specified field.
     *
     * @param field Field.
     * @return True if splitter serialization is supported, false otherwise.
     */
    private boolean supports(Field field) {
        if (field.getDeclaredAnnotation(SerializeSeparately.class) == null)
            return false;

        if (IgniteFeatures.allNodesSupport(ctx.config().getDiscoverySpi(), IgniteFeatures.SPLITTED_CACHE_CONFIGURATIONS_V2))
            return true;

        for (String filedName : FIELDS_V1) {
            if (filedName.equals(field.getName()))
                return true;
        }

        return false;
    }

    /**
     * @param fieldName Field name to serialize.
     * @param val Field value.
     *
     * @return Serialized value.
     */
    private byte[] serialize(String fieldName, Object val) {
        try {
            return U.marshal(marshaller, val);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to serialize field [fieldName=" + fieldName + ", value=" + val + ']', e);
        }
    }
}
