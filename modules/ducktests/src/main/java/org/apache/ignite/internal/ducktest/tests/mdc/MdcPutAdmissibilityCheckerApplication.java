package org.apache.ignite.internal.ducktest.tests.mdc;

import javax.cache.CacheException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.ducktest.tests.dto.IndexedDataRecord;

/**
 * Checks whether the put load is admissible or inadmissible for the DC this client
 * belongs to.
 * <p>
 * With {@code expectAdmissible=true} every probe put must succeed (primary DC).
 * With {@code expectAdmissible=false} every probe put must fail with a
 * {@link CacheException} thrown by the topology validator (read-only DC).
 * <p>
 * Probe keys start at {@code keyOffset} (defaults to 1_000_000) so they never
 * intersect with the data set verified by {@link MdcDataCheckerApplication}.
 */
public class MdcPutAdmissibilityCheckerApplication extends MdcCacheAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) throws IgniteInterruptedCheckedException {
        String cacheName = jNode.path("cacheName").asText(DFLT_CACHE_NAME);
        boolean expectAdmissible = jNode.get("expectAdmissible").asBoolean();
        int probes = jNode.path("probes").asInt(100);
        int keyOffset = jNode.path("keyOffset").asInt(1_000_000);

        markInitialized();
        waitForActivation();

        IgniteCache<Integer, IndexedDataRecord> cache = mdcCache(cacheName);

        log.info("Put admissibility check started [dc=" + dcId() + ", cache=" + cache.getName() +
            ", expectAdmissible=" + expectAdmissible + ", probes=" + probes + "]");

        int succeeded = 0;
        int rejected = 0;

        for (int i = 0; i < probes && !terminated(); i++) {
            int key = keyOffset + i;

            try {
                cache.put(key, new IndexedDataRecord(key));

                succeeded++;

                if (!expectAdmissible)
                    log.error("Put unexpectedly succeeded in read-only DC [dc=" + dcId() + ", key=" + key + "]");
            }
            catch (CacheException e) {
                rejected++;

                if (expectAdmissible)
                    log.error("Put unexpectedly rejected [dc=" + dcId() + ", key=" + key + "]", e);
                else
                    log.info("Put rejected as expected [dc=" + dcId() + ", key=" + key +
                        ", msg=" + e.getMessage() + "]");
            }
        }

        if (expectAdmissible && rejected > 0) {
            throw new IllegalStateException("Put load is inadmissible while expected to be admissible [dc=" +
                dcId() + ", succeeded=" + succeeded + ", rejected=" + rejected + "]");
        }

        if (!expectAdmissible && succeeded > 0) {
            throw new IllegalStateException("Put load is admissible while expected to be inadmissible [dc=" +
                dcId() + ", succeeded=" + succeeded + ", rejected=" + rejected + "]");
        }

        log.info("Put admissibility check passed [dc=" + dcId() +
            ", succeeded=" + succeeded + ", rejected=" + rejected + "]");

        markFinished();
    }
}
