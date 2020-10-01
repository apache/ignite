package org.apache.ignite.internal.ducktest.tests.start_stop_client;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

import java.util.UUID;

public class SingleClientNode  extends IgniteAwareApplication {

    private IgniteCache<String, String> cache;
    private String cacheName;
    private String reportName;
    private long pacing = 0;

    @Override
    protected void run(JsonNode jsonNode) throws Exception {
        cacheName = jsonNode.get("cacheName").asText();
        reportName = jsonNode.get("reportName").asText();
        pacing = jsonNode.get("pacing").asLong();

        log.info("test props: " + "cacheName=" + cacheName + " reportName=" + reportName + " pacing=" + pacing);

        cache = ignite.getOrCreateCache(prepareCacheConfiguration(cacheName));
        log.info("nodeId: " + ignite.name() + " starting cache operations");

        markInitialized();
        while (!terminated()){
            cacheOperation();
        }
        markFinished();
    }

    private CacheConfiguration prepareCacheConfiguration(String cacheName){
        CacheConfiguration cfg = new CacheConfiguration();
        cfg.setBackups(2);
        cfg.setName(cacheName);
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        return cfg;
    }

    private long cacheOperation() throws InterruptedException {
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
