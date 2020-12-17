package org.apache.ignite.internal.ducktest.tests.pds_compatibility_test;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.UUID;

public class DictionaryCacheApplication extends IgniteAwareApplication {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void run(JsonNode jsonNode) {
        log.info("Creating cache...");

        CacheConfiguration<Long, String> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setName(jsonNode.get("cacheName").asText())
                .setCacheMode(CacheMode.REPLICATED)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setIndexedTypes(Long.class, String.class);

        IgniteCache<Long, String> cache = ignite.getOrCreateCache(cacheCfg);

        for (long i = 0; i < jsonNode.get("range").asLong(); i++) {
            String uuid = UUID.randomUUID().toString();
            cache.put(i, uuid);
        }
        log.info("Cache created");
        markSyncExecutionComplete();
    }
}
