package org.apache.ignite.internal.ducktest.tests.pds_compatibility_test;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

public class DictionaryCacheApplicationCheck extends IgniteAwareApplication {
    /**
     * {@inheritDoc}
     */
    @Override protected void run(JsonNode jsonNode) {
        log.info("Opening cache...");

        IgniteCache<Long, String> cache = ignite.cache(jsonNode.get("cacheName").asText());

        for (long i = 0; i < jsonNode.get("range").asLong(); i++) {
            cache.get(i);
        }
        log.info("Cache checked");
        markSyncExecutionComplete();
    }
}
