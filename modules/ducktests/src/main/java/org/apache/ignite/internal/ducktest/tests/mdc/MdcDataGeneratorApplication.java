package org.apache.ignite.internal.ducktest.tests.mdc;

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

        markInitialized();
        waitForActivation();

        IgniteCache<Integer, IndexedDataRecord> cache = mdcCache(jNode);

        log.info("Data generation started [dc=" + dcId() + ", cache=" + cache.getName() +
            ", from=" + from + ", to=" + to + "]");

        for (int i = from; i < to && !terminated(); i++)
            cache.put(i, new IndexedDataRecord(i));

        log.info("Data generation finished [dc=" + dcId() + ", entries=" + (to - from) + "]");

        markFinished();
    }
}
