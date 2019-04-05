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
import org.apache.ignite.configuration.SerializeSeparately;
import org.apache.ignite.internal.util.typedef.T2;

/**
 *
 */
public class CacheConfigurationSplitter {
    /** Cache configuration to hold default values for configuration fields. */
    private static final CacheConfiguration<?, ?> DEFAULT_CACHE_CONFIG = new CacheConfiguration<>();

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
                return new T2<>(CacheConfigurationEnricher.enrichFully(desc.config(), desc.cacheConfigEnrichment()), null);
            else
                return new T2<>(desc.config(), null);
        }

        if (desc.isConfigurationEnriched())
            return split(desc.config());

        return new T2<>(desc.config(), desc.cacheConfigEnrichment());
    }

    /**
     * @param desc Description.
     */
    public T2<CacheConfiguration, CacheConfigurationEnrichment> split(DynamicCacheDescriptor desc) {
        if (!splitAllowed) {
            if (!desc.isConfigurationEnriched())
                return new T2<>(CacheConfigurationEnricher.enrichFully(desc.cacheConfiguration(), desc.cacheConfigEnrichment()), null);
            else
                return new T2<>(desc.cacheConfiguration(), null);
        }

        if (desc.isConfigurationEnriched())
            return split(desc.cacheConfiguration());

        return new T2<>(desc.cacheConfiguration(), desc.cacheConfigEnrichment());
    }

    /**
     * @param ccfg Ccfg.
     */
    public T2<CacheConfiguration, CacheConfigurationEnrichment> split(CacheConfiguration ccfg) {
        Map<String, byte[]> enrichment = new HashMap<>();
        Map<String, String> fieldClassNames = new HashMap<>();

        CacheConfiguration shrinkedCopy = new CacheConfiguration(ccfg);

        try {
            for (Field field : CacheConfiguration.class.getDeclaredFields()) {
                if (field.getDeclaredAnnotation(SerializeSeparately.class) != null) {
                    field.setAccessible(true);

                    Object val = field.get(shrinkedCopy);

                    byte[] serializedVal = serialize(val);

                    enrichment.put(field.getName(), serializedVal);

                    fieldClassNames.put(field.getName(), val != null ? val.getClass().getName() : null);

                    // Replace with default value.
                    field.set(shrinkedCopy, field.get(DEFAULT_CACHE_CONFIG));
                }
            }
        }
        catch (Exception e) {
            throw new IgniteException("Failed to split cache configuration", e);
        }

        return new T2<>(shrinkedCopy, new CacheConfigurationEnrichment(enrichment, fieldClassNames));
    }

    /**
     * @param val Value.
     */
    private byte[] serialize(Object val) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        try (ObjectOutputStream oos = new ObjectOutputStream(os)) {
            oos.writeObject(val);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to serialize field", e);
        }

        return os.toByteArray();
    }
}
