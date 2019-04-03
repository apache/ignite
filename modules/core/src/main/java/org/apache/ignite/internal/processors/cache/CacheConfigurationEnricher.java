package org.apache.ignite.internal.processors.cache;

import java.lang.reflect.Field;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.SerializeSeparately;

public class CacheConfigurationEnricher {
    public static CacheConfiguration<?, ?> enrich(
        GridCacheSharedContext cctx,
        CacheConfiguration<?, ?> ccfg,
        CacheConfigurationEnrichment enrichment
    ) {
        CacheConfiguration<?, ?> enrichedCopy = new CacheConfiguration<>(ccfg);

        try {
            for (Field field : CacheConfiguration.class.getDeclaredFields())
                if (field.getDeclaredAnnotation(SerializeSeparately.class) != null) {
                    field.setAccessible(true);

                    Object enrichedValue = enrichment.getFieldValue(field.getName());

                    field.set(enrichedCopy, enrichedValue);
                }
        }
        catch (Exception e) {
            throw new IgniteException("Failed to enrich cache configuration " + ccfg.getName(), e);
        }

        return enrichedCopy;
    }

    public static CacheConfiguration<?, ?> enrichFully(
        CacheConfiguration<?, ?> ccfg,
        CacheConfigurationEnrichment enrichment
    ) {
        CacheConfiguration<?, ?> enrichedCopy = new CacheConfiguration<>(ccfg);

        try {
            for (Field field : CacheConfiguration.class.getDeclaredFields())
                if (field.getDeclaredAnnotation(SerializeSeparately.class) != null) {
                    field.setAccessible(true);

                    Object enrichedValue = enrichment.getFieldValue(field.getName());

                    field.set(enrichedCopy, enrichedValue);
                }
        }
        catch (Exception e) {
            throw new IgniteException("Failed to enrich cache configuration " + ccfg.getName(), e);
        }

        return enrichedCopy;
    }
}
