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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.SerializeSeparately;
import org.apache.ignite.internal.util.typedef.T2;

/**
 *
 */
public class CacheConfigurationSplitter {
    /** Cache configuration to hold default field values. */
    private static final CacheConfiguration<?, ?> DEFAULT_CACHE_CONFIG = new CacheConfiguration<>();

    /** Near cache configuration to hold default field values. */
    private static final NearCacheConfiguration<?, ?> DEFAULT_NEAR_CACHE_CONFIG = new NearCacheConfiguration<>();

    /** If {@code false} object will return full cache configurations,
     * in other case it will return splitted cache configurations.  */
    private final boolean splitAllowed;

    /**
     * @param splitAllowed Split allowed.
     */
    public CacheConfigurationSplitter(boolean splitAllowed) {
        this.splitAllowed = splitAllowed;
    }

    /**
     * @param desc Description.
     */
    public T2<CacheConfiguration, CacheConfigurationEnrichment> split(CacheGroupDescriptor desc) {
        if (!splitAllowed) {
            if (!desc.isConfigurationEnriched())
                return new T2<>(CacheConfigurationEnricher.enrichFully(desc.config(), desc.cacheConfigurationEnrichment()), null);
            else
                return new T2<>(desc.config(), null);
        }

        if (desc.isConfigurationEnriched())
            return split(desc.config());

        return new T2<>(desc.config(), desc.cacheConfigurationEnrichment());
    }

    /**
     * @param desc Description.
     */
    public T2<CacheConfiguration, CacheConfigurationEnrichment> split(DynamicCacheDescriptor desc) {
        if (!splitAllowed) {
            if (!desc.isConfigurationEnriched())
                return new T2<>(CacheConfigurationEnricher.enrichFully(desc.cacheConfiguration(), desc.cacheConfigurationEnrichment()), null);
            else
                return new T2<>(desc.cacheConfiguration(), null);
        }

        if (desc.isConfigurationEnriched())
            return split(desc.cacheConfiguration());

        return new T2<>(desc.cacheConfiguration(), desc.cacheConfigurationEnrichment());
    }

    /**
     * @param ccfg Cache configuration.
     */
    public T2<CacheConfiguration, CacheConfigurationEnrichment> split(CacheConfiguration ccfg) {
        try {
            CacheConfiguration cfgCopy = new CacheConfiguration(ccfg);

            CacheConfigurationEnrichment enrichment = buildEnrichment(CacheConfiguration.class, cfgCopy, DEFAULT_CACHE_CONFIG);

            if (ccfg.getNearConfiguration() != null) {
                NearCacheConfiguration nearCfgCopy = new NearCacheConfiguration(ccfg.getNearConfiguration());

                CacheConfigurationEnrichment nearEnrichment = buildEnrichment(
                        NearCacheConfiguration.class, nearCfgCopy, DEFAULT_NEAR_CACHE_CONFIG);

                enrichment.nearCacheConfigurationEnrichment(nearEnrichment);

                cfgCopy.setNearConfiguration(nearCfgCopy);
            }

            return new T2<>(cfgCopy, enrichment);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to split cache configuration", e);
        }
    }

    /**
     * Builds {@link CacheConfigurationEnrichment} from given config.
     * It extracts all field values to enrichment object replacing values of that fields with default.
     *
     * @param configClass Configuration class.
     * @param config Configuration to build enrichment from.
     * @param defaultConfig Default configuration to replace enriched values with default.
     * @param <T> Configuration class.
     * @return Enrichment object for given config.
     * @throws IllegalAccessException If failed.
     */
    private <T> CacheConfigurationEnrichment buildEnrichment(
        Class<T> configClass,
        T config,
        T defaultConfig
    ) throws IllegalAccessException {
        Map<String, byte[]> enrichment = new HashMap<>();
        Map<String, String> fieldClassNames = new HashMap<>();

        for (Field field : configClass.getDeclaredFields()) {
            if (field.getDeclaredAnnotation(SerializeSeparately.class) != null) {
                field.setAccessible(true);

                Object val = field.get(config);

                byte[] serializedVal = serialize(field.getName(), val);

                enrichment.put(field.getName(), serializedVal);

                fieldClassNames.put(field.getName(), val != null ? val.getClass().getName() : null);

                // Replace with default value.
                field.set(config, field.get(defaultConfig));
            }
        }

        return new CacheConfigurationEnrichment(enrichment, fieldClassNames);
    }

    /**
     * @param val Value.
     */
    private byte[] serialize(String fieldName, Object val) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        try (ObjectOutputStream oos = new ObjectOutputStream(os)) {
            oos.writeObject(val);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to serialize field [fieldName=" + fieldName + ", value=" + val + ']', e);
        }

        return os.toByteArray();
    }
}
