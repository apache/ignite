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
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;

/**
 *
 */
public class CacheConfigurationSplitterImpl implements CacheConfigurationSplitter {
    /** Cache configuration to hold default field values. */
    private static final CacheConfiguration<?, ?> DEFAULT_CACHE_CONFIG = new CacheConfiguration<>();

    /** Near cache configuration to hold default field values. */
    private static final NearCacheConfiguration<?, ?> DEFAULT_NEAR_CACHE_CONFIG = new NearCacheConfiguration<>();

    /** Marshaller. */
    private final Marshaller marshaller;

    /**
     * @param marshaller Marshaller.
     */
    public CacheConfigurationSplitterImpl(Marshaller marshaller) {
        this.marshaller = marshaller;
    }

    /**
     * @param ccfg Cache configuration.
     */
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
            if (field.getDeclaredAnnotation(SerializeSeparately.class) != null) {
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
     * @param val Value.
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
