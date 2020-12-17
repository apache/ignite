package org.apache.ignite.internal.ducktest.tests.pds_compatibility_test;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import com.fasterxml.jackson.databind.JsonNode;


import java.util.UUID;

public class SqlCacheApplication extends IgniteAwareApplication {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void run(JsonNode jsonNode) {
        log.info("Creating cache...");

        CacheConfiguration<Long, Account> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setName(jsonNode.get("cacheName").asText())
                .setCacheMode(CacheMode.PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(3)
                .setIndexedTypes(Long.class, Account.class);

        IgniteCache<Long, Account> cache = ignite.getOrCreateCache(cacheCfg);

        for (long i = 0; i < jsonNode.get("range").asLong(); i++) {
            String uuid = UUID.randomUUID().toString();
            cache.put(i, new Account(
                    uuid, uuid, uuid, uuid, uuid, uuid,
                    uuid, uuid, uuid, i, i));
        }

        log.info("Cache created");
        markSyncExecutionComplete();
    }
}