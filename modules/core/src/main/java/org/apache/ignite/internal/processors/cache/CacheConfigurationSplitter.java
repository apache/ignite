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

public class CacheConfigurationSplitter {
    /** Cache configuration to hold default values for fields. */
    private static final CacheConfiguration<?, ?> DEFAULT = new CacheConfiguration<>();

    private final boolean splitAllowed;

    public CacheConfigurationSplitter(boolean splitAllowed) {
        this.splitAllowed = splitAllowed;
    }

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

    public T2<CacheConfiguration, CacheConfigurationEnrichment> split(CacheConfiguration ccfg) {
        Map<String, byte[]> enrichment = new HashMap<>();
        Map<String, String> fieldClassNames = new HashMap<>();

        CacheConfiguration copy = new CacheConfiguration(ccfg);

        try {
            for (Field field : CacheConfiguration.class.getDeclaredFields()) {
                if (field.getDeclaredAnnotation(SerializeSeparately.class) != null) {
                    field.setAccessible(true);

                    Object val = field.get(copy);

                    byte[] serializedVal = serialize(val);

                    enrichment.put(field.getName(), serializedVal);

                    fieldClassNames.put(field.getName(), val != null ? val.getClass().getName() : "null");

                    // Replace with default value.
                    field.set(copy, field.get(DEFAULT));
                }
            }
        }
        catch (Exception e) {
            throw new IgniteException("Failed to split cache configuration", e);
        }

        return new T2<>(copy, new CacheConfigurationEnrichment(enrichment, fieldClassNames));
    }

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
