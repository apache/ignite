package org.apache.ignite.internal.ducktest.tests.mdc;

import java.util.Map;
import java.util.TreeMap;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.ducktest.tests.dto.IndexedDataRecord;

/**
 * Populates the MDC cache with a deterministic data set: keys in {@code [from, to)},
 * values {@code IndexedDataRecord(key)}. Run it before the network partition.
 */
public class MdcDataGeneratorApplication extends MdcCacheAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) throws IgniteInterruptedCheckedException {
        int from = jNode.path("from").asInt(0);
        int to = jNode.path("to").asInt(10_000);
        int batchSize = jNode.path("batchSize").asInt(1_024);

        markInitialized();
        waitForActivation();

        IgniteCache<Integer, IndexedDataRecord> cache = mdcCache(jNode);

        log.info("Data generation started [dc=" + dcId() + ", cache=" + cache.getName() +
            ", from=" + from + ", to=" + to + "]");

        Map<Integer, IndexedDataRecord> batch = new TreeMap<>();

        for (int i = from; i < to && !terminated(); i++) {
            batch.put(i, new IndexedDataRecord(i));

            if (batch.size() >= batchSize) {
                cache.putAll(batch);
                batch.clear();
            }
        }

        if (!batch.isEmpty() && !terminated())
            cache.putAll(batch);

        log.info("Data generation finished [dc=" + dcId() + ", entries=" + (to - from) + "]");

        markFinished();
    }
}
