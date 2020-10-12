package org.apache.ignite.internal.ducktest.tests;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * https://sbtatlas.sigma.sbrf.ru/jira/browse/IGN-1794
 * create and destroy a cache with specific name to check for memory leak
 */
public class CreateDestroyCache extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) {
        markInitialized();

        for (int i = 0; i < jsonNode.get("cacheNumber").asInt(); i++) {
            log.info("Creating cache " + i + "...");
            ignite.createCache(jsonNode.get("cacheName").asText());

            log.info("Destroying cache " + i + "...");
            ignite.destroyCache(jsonNode.get("cacheName").asText());
        }

        markFinished();
    }
}
