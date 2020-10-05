package org.apache.ignite.internal.ducktest.tests.start_stop_client;

import java.util.Optional;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Simple client node. Tx-put operation.
 */
public class SingleClientNode  extends IgniteAwareApplication {
    /** */
    private String cacheName;

    /** */
    private long pacing = 0;

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        cacheName = jsonNode.get("cacheName").asText();

        pacing = Optional.ofNullable(jsonNode.get("pacing"))
                .map(JsonNode::asLong)
                .orElse(0l);

        log.info("test props: cacheName=" + cacheName + " pacing=" + pacing);

        IgniteCache<String, String> cache = ignite.getOrCreateCache(prepareCacheConfiguration(cacheName));
        log.info("nodeId: " + ignite.name() + " starting cache operations");

        markInitialized();
        while (!terminated()){
            cacheOperation(cache);
        }
        markFinished();
    }

    /** */
    private CacheConfiguration prepareCacheConfiguration(String cacheName){
        CacheConfiguration cfg = new CacheConfiguration();
        cfg.setBackups(2);
        cfg.setName(cacheName);
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        return cfg;
    }

    /** */
    private long cacheOperation(IgniteCache<String, String> cache) throws InterruptedException {
        String key = UUID.randomUUID().toString();
        String value = UUID.randomUUID().toString();
        long startTime = System.nanoTime();
        cache.put(key,value);
        long resultTime = System.nanoTime() - startTime;
        log.info("success put key=" + key + " value=" + value + " latency: " + resultTime + "ns");
        Thread.sleep(pacing);
        return resultTime;
    }
}
