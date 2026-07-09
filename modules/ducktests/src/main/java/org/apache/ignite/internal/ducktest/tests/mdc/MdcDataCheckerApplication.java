package org.apache.ignite.internal.ducktest.tests.mdc;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.ducktest.tests.dto.IndexedDataRecord;

/**
 * Verifies that all entries written by {@link MdcDataGeneratorApplication} are readable
 * and hold expected values. Intended to be run from a client in each DC after the
 * network partition: reads must succeed everywhere, even in the read-only DC.
 */
public class MdcDataCheckerApplication extends MdcCacheAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) throws IgniteInterruptedCheckedException {
        String cacheName = jNode.get("cacheName").asText();
        int from = jNode.path("from").asInt(0);
        int to = jNode.path("to").asInt(10_000);

        markInitialized();
        waitForActivation();

        IgniteCache<Integer, IndexedDataRecord> cache = mdcCache(cacheName);

        log.info("Data check started [dc=" + dcId() + ", cache=" + cache.getName() +
            ", from=" + from + ", to=" + to + "]");

        int missed = 0;
        int corrupted = 0;

        for (int i = from; i < to && !terminated(); i++) {
            IndexedDataRecord obj = cache.get(i);

            if (obj == null) {
                missed++;

                log.error("Entry is missed [dc=" + dcId() + ", key=" + i + "]");
            }
            else if (!obj.equals(new IndexedDataRecord(i))) {
                corrupted++;

                log.error("Entry is corrupted [dc=" + dcId() + ", key=" + i + ", val=" + obj + "]");
            }
        }

        if (missed > 0 || corrupted > 0) {
            throw new IllegalStateException("Data check failed [dc=" + dcId() +
                ", missed=" + missed + ", corrupted=" + corrupted + "]");
        }

        log.info("Data check passed [dc=" + dcId() + ", entries=" + (to - from) + "]");

        markFinished();
    }
}
