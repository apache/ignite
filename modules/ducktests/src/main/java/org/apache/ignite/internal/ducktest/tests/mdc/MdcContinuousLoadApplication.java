/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.ducktest.tests.mdc;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import javax.cache.CacheException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.ducktest.tests.dto.IndexedDataRecord;
import org.apache.ignite.internal.ducktest.utils.OpStats;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.internal.ducktest.utils.Utils.fmtMs;
import static org.apache.ignite.internal.ducktest.utils.Utils.getEnum;
import static org.apache.ignite.internal.ducktest.utils.Utils.timed;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Universal MDC load application. Runs a single-threaded synchronous load of the
 * requested {@link LoadMode} either for a fixed number of iterations (a "burst") or until
 * externally terminated (a "background" load spanning several test phases, e.g. a
 * network partition and its healing).
 * <p>
 * Parameters:
 * <ul>
 *     <li>{@code mode} - one of {@code GET, PUT, TX_PUT, SQL_SELECT, SQL_PUT};</li>
 *     <li>{@code cacheName} - cache to operate on;</li>
 *     <li>{@code createCache} - if {@code true}, (re)creates the cache from the MDC
 *         configuration parameters (see {@link MdcCacheAwareApplication}); otherwise the
 *         cache must already exist;</li>
 *     <li>{@code keyFrom} / {@code keyTo} - key range. Read modes cycle within the range;
 *         write modes advance sequentially from {@code keyFrom} on every success, so on
 *         completion the keys {@code [keyFrom, keyFrom + opsCnt)} are guaranteed written;</li>
 *     <li>{@code iterations} - number of operations; {@code 0} means "run until terminated";</li>
 *     <li>{@code inadmissible} - write modes only. {@code false} (default): writes must
 *         succeed; {@code true}: every write must be rejected by the topology validator
 *         (read-only DC), any success fails the application;</li>
 *     <li>{@code stopOnError} - if {@code true}, the load stops gracefully on the very first
 *         operation failure (rather than failing the application), records the number of
 *         successful operations and a {@code StoppedOnError} flag, and finishes. Takes precedence
 *         over {@code continueOnError}. Intended for a load that must be cut off by the first
 *         exception a network partition triggers;</li>
 *     <li>{@code continueOnError} - if {@code true}, operation failures are counted instead of
 *         failing fast. Intended for background loads crossing a partition boundary, where a
 *         short transient error window is possible;</li>
 *     <li>{@code opPauseMs} - pause between operations, default 0;</li>
 *     <li>{@code resultPrefix} - prefix for recorded results, so that several runs reusing one
 *         service produce uniquely named results;</li>
 *     <li>{@code txConcurrency} / {@code txIsolation} - transaction parameters for {@code TX_PUT}.</li>
 * </ul>
 */
public class MdcContinuousLoadApplication extends MdcCacheAwareApplication {
    /** */
    public static final TransactionConcurrency DFLT_TX_CONCURRENCY = PESSIMISTIC;

    /** */
    public static final TransactionIsolation DFLT_TX_ISOLATION = REPEATABLE_READ;

    /** Cache for the cache API modes ({@code null} in SQL modes). */
    private IgniteCache<Integer, IndexedDataRecord> cache;

    /** Cache for the SQL modes ({@code null} in cache API modes). */
    private IgniteCache<Integer, Integer> sqlCache;

    /** Compiled DML statement for {@link LoadMode#SQL_PUT}. */
    private String mergeSql;

    /** Compiled query for {@link LoadMode#SQL_SELECT}. */
    private String selectSql;

    /** Latency of successful operations. */
    private final OpStats stats = new OpStats();

    /** */
    private TransactionConcurrency txConcurrency;

    /** */
    private TransactionIsolation txIsolation;

    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) throws Exception {
        LoadMode mode = getEnum(jNode, "mode", LoadMode.class);

        String cacheName = jNode.path("cacheName").asText(DFLT_CACHE_NAME);
        boolean createCache = jNode.path("createCache").asBoolean(false);

        int keyFrom = jNode.path("keyFrom").asInt(0);
        int keyTo = jNode.path("keyTo").asInt(Integer.MAX_VALUE);
        long iterations = jNode.path("iterations").asLong(0);

        boolean inadmissible = jNode.path("inadmissible").asBoolean(false);
        boolean continueOnError = jNode.path("continueOnError").asBoolean(false);
        boolean stopOnError = jNode.path("stopOnError").asBoolean(false);

        long opPauseMs = jNode.path("opPauseMs").asLong(0);

        String pfx = jNode.path("resultPrefix").asText("");

        txConcurrency = getEnum(jNode, "txConcurrency", DFLT_TX_CONCURRENCY);
        txIsolation = getEnum(jNode, "txIsolation", DFLT_TX_ISOLATION);

        markInitialized();
        waitForActivation();

        if (mode.isSql())
            sqlCache = createCache ? mdcSqlCache(jNode) : ignite.cache(cacheName);
        else
            cache = createCache ? mdcCache(jNode) : ignite.cache(cacheName);

        mergeSql = String.format("MERGE INTO \"%s\".%s(_KEY, _VAL) VALUES(?, ?)", cacheName, SQL_TABLE);
        selectSql = String.format("SELECT _VAL FROM \"%s\".%s WHERE _KEY = ?", cacheName, SQL_TABLE);

        log.info("MDC load started [dc=" + dcId() + ", mode=" + mode + ", cache=" + cacheName +
            ", keyFrom=" + keyFrom + ", keyTo=" + keyTo + ", iterations=" + iterations +
            ", inadmissible=" + inadmissible + ", continueOnError=" + continueOnError + "]");

        long opsCnt = 0;
        long errCnt = 0;

        boolean stoppedOnError = false;

        long maxStallMs = 0;

        long startTs = System.currentTimeMillis();
        long lastOkTs = startTs;

        int key = keyFrom;

        while (!terminated() && (iterations == 0 || opsCnt + errCnt < iterations)) {
            boolean ok = true;

            try {
                doOperation(mode, key);
            }
            catch (CacheException | IgniteException e) {
                ok = false;

                if (inadmissible)
                    log.info("Write rejected as expected [dc=" + dcId() + ", key=" + key +
                        ", msg=" + e.getMessage() + "]");
                else if (stopOnError) {
                    log.warn("Operation failed, cutting the load on first error [dc=" + dcId() + ", mode=" + mode +
                        ", key=" + key + ", succeeded=" + opsCnt + ", msg=" + e.getMessage() + "]", e);

                    errCnt++;
                    stoppedOnError = true;

                    break;
                }
                else if (continueOnError)
                    log.warn("Operation failed, tolerated [dc=" + dcId() + ", mode=" + mode +
                        ", key=" + key + ", msg=" + e.getMessage() + "]");
                else
                    throw new IllegalStateException("Operation failed [dc=" + dcId() + ", mode=" + mode +
                        ", key=" + key + "]", e);
            }

            if (ok) {
                opsCnt++;

                long now = System.currentTimeMillis();

                maxStallMs = Math.max(maxStallMs, now - lastOkTs);
                lastOkTs = now;
            }
            else
                errCnt++;

            // Writes advance on success only, so [keyFrom, keyFrom + opsCnt) is guaranteed written.
            // The inadmissible-probe mode advances always to probe distinct keys. Reads cycle the range.
            if (ok || (mode.isWrite() && inadmissible)) {
                key++;

                if (key >= keyTo)
                    key = keyFrom;
            }

            if (opPauseMs > 0)
                Thread.sleep(opPauseMs);
        }

        long durationMs = System.currentTimeMillis() - startTs;

        if (mode.isWrite() && inadmissible && opsCnt > 0) {
            throw new IllegalStateException("Write load is admissible while expected to be inadmissible [dc=" +
                dcId() + ", mode=" + mode + ", succeeded=" + opsCnt + ", rejected=" + errCnt + "]");
        }

        recordResult(pfx + "OpsCnt", String.valueOf(opsCnt));
        recordResult(pfx + "ErrCnt", String.valueOf(errCnt));
        recordResult(pfx + "StoppedOnError", String.valueOf(stoppedOnError));
        recordResult(pfx + "DurationMs", String.valueOf(durationMs));

        recordResult(pfx + "AvgOpMs", fmtMs(stats.avgNs()));
        recordResult(pfx + "MinOpMs", fmtMs(stats.minNs()));
        recordResult(pfx + "MaxOpMs", fmtMs(stats.maxNs()));

        recordResult(pfx + "DerivedTps", String.format(Locale.US, "%.1f", stats.tps()));

        recordResult(pfx + "MaxStallMs", String.valueOf(maxStallMs));

        log.info("MDC load finished [dc=" + dcId() + ", mode=" + mode + ", ops=" + opsCnt +
            ", errs=" + errCnt + ", stoppedOnError=" + stoppedOnError + ", durationMs=" + durationMs +
            ", avgOpMs=" + fmtMs(stats.avgNs()) + ", maxOpMs=" + fmtMs(stats.maxNs()) +
            ", maxStallMs=" + maxStallMs + "]");

        markFinished();
    }

    /**
     * Executes a single operation of the given mode against the given key. Throws a
     * {@link CacheException} or {@link IgniteException} on operation failure (e.g. a write
     * rejected by the topology validator).
     */
    private void doOperation(LoadMode mode, int key) {
        switch (mode) {
            case GET: {
                IndexedDataRecord val = timed(stats, () -> cache.get(key));

                if (val == null || !val.equals(new IndexedDataRecord(key)))
                    throw new IgniteException("Read entry is missed or corrupted [dc=" + dcId() + ", key=" + key +
                        ", val=" + val + "]");

                break;
            }

            case PUT: {
                IndexedDataRecord val = new IndexedDataRecord(key);

                timed(stats, () -> cache.put(key, val));

                break;
            }

            case TX_PUT: {
                IndexedDataRecord val = new IndexedDataRecord(key);

                timed(stats, () -> {
                    try (Transaction tx = ignite.transactions().txStart(txConcurrency, txIsolation)) {
                        cache.put(key, val);

                        tx.commit();
                    }
                });

                break;
            }

            case SQL_PUT: {
                SqlFieldsQuery qry = new SqlFieldsQuery(mergeSql).setArgs(key, key);

                timed(stats, () -> sqlCache.query(qry).getAll());

                break;
            }

            case SQL_SELECT: {
                SqlFieldsQuery qry = new SqlFieldsQuery(selectSql).setArgs(key);

                List<List<?>> rows = timed(stats, () -> sqlCache.query(qry).getAll());

                if (rows.isEmpty() || !Objects.equals(rows.get(0).get(0), key))
                    throw new IgniteException("SQL row is missed or corrupted [dc=" + dcId() + ", key=" + key +
                        ", rows=" + rows + "]");

                break;
            }

            default:
                throw new IllegalArgumentException("Unknown mode: " + mode);
        }
    }
}
