package org.apache.ignite.internal.ducktest.tests.mdc;

import javax.cache.CacheException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.internal.ducktest.tests.dto.IndexedDataRecord;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.ducktest.utils.OpStats;

import static org.apache.ignite.internal.ducktest.utils.Utils.fmtMs;
import static org.apache.ignite.internal.ducktest.utils.Utils.timed;

/**
 * Thin client MDC load application.
 * <p>
 * The application performs synchronous {@code GET} or {@code PUT} operations against an
 * existing cache and records the average operation latency, which the python side uses as
 * a crude proxy for "which data center served the request": with a high cross-DC netem
 * delay, DC-local routing yields a small average, cross-DC routing a large one.
 * <p>
 * Parameters: {@code mode} ({@code GET}/{@code PUT}), {@code cacheName}, {@code keyFrom},
 * {@code keyTo}, {@code iterations}, {@code expectAdmissible} (PUT only),
 * {@code resultPrefix}.
 * <p>
 */
public class MdcThinClientLoadApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) throws Exception {
        String mode = jNode.get("mode").asText().toUpperCase();
        String cacheName = jNode.get("cacheName").asText();

        int keyFrom = jNode.path("keyFrom").asInt(0);
        int keyTo = jNode.path("keyTo").asInt(Integer.MAX_VALUE);
        long iterations = jNode.path("iterations").asLong(100);

        boolean put = "PUT".equals(mode);
        boolean expectAdmissible = jNode.path("expectAdmissible").asBoolean(true);

        String pfx = jNode.path("resultPrefix").asText("");

        markInitialized();

        ClientCache<Integer, IndexedDataRecord> cache = client.cache(cacheName);

        log.info("MDC thin client load started [mode=" + mode + ", cache=" + cacheName +
            ", keyFrom=" + keyFrom + ", keyTo=" + keyTo + ", iterations=" + iterations +
            ", expectAdmissible=" + expectAdmissible + "]");

        long opsCnt = 0;
        long errCnt = 0;

        OpStats stats = new OpStats();

        long startTs = System.currentTimeMillis();

        int key0 = keyFrom;

        for (long i = 0; i < iterations && !terminated(); i++) {
            int key = key0;

            boolean ok;

            try {
                if (put) {
                    IndexedDataRecord val = new IndexedDataRecord(key);

                    timed(stats, () -> cache.put(key, val));

                    ok = true;
                }
                else {
                    IndexedDataRecord val = timed(stats, () -> cache.get(key));

                    ok = val != null && val.equals(new IndexedDataRecord(key));

                    if (!ok)
                        log.error("Read entry is missed or corrupted [key=" + key + ", val=" + val + "]");
                }
            }
            catch (ClientException | CacheException e) {
                ok = false;

                if (put && !expectAdmissible)
                    log.info("Put rejected as expected [key=" + key + ", msg=" + e.getMessage() + "]");
                else
                    throw new IllegalStateException("Operation failed [mode=" + mode + ", key=" + key + "]", e);
            }

            if (ok)
                opsCnt++;
            else
                errCnt++;

            // Writes advance always here: the inadmissible probe covers distinct keys, and for
            // an admissible run any failure fails fast above, so success and attempt counts match.
            key0++;

            if (key0 >= keyTo)
                key0 = keyFrom;
        }

        long durationMs = System.currentTimeMillis() - startTs;

        if (put && !expectAdmissible && opsCnt > 0) {
            throw new IllegalStateException("Put load is admissible while expected to be inadmissible " +
                "[succeeded=" + opsCnt + ", rejected=" + errCnt + "]");
        }

        recordResult(pfx + "OpsCnt", String.valueOf(opsCnt));
        recordResult(pfx + "ErrCnt", String.valueOf(errCnt));

        recordResult(pfx + "AvgOpMs", fmtMs(stats.avgNs()));
        recordResult(pfx + "MinOpMs", fmtMs(stats.minNs()));
        recordResult(pfx + "MaxOpMs", fmtMs(stats.maxNs()));

        log.info("MDC thin client load finished [mode=" + mode + ", ops=" + opsCnt +
            ", errs=" + errCnt + ", durationMs=" + durationMs +
            ", avgOpMs=" + fmtMs(stats.avgNs()) + "]");

        markFinished();
    }
}
