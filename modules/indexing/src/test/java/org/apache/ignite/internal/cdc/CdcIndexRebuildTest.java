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

package org.apache.ignite.internal.cdc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cdc.AbstractCdcTest;
import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.cdc.CdcMain.COMMITTED_SEG_IDX;
import static org.apache.ignite.internal.cdc.CdcMain.CUR_SEG_IDX;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.util.IgniteUtils.MB;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class CdcIndexRebuildTest extends AbstractCdcTest {
    /** */
    public static final int VALS_CNT = 1024 * 30;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setCdcEnabled(true))
            .setWalSegmentSize((int)(16 * MB))
        );
    }

    /** */
    @Test
    public void testIndexRebuild() throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        IgniteCache<Integer, TestVal> cache = srv.getOrCreateCache(
            new CacheConfiguration<Integer, TestVal>(DEFAULT_CACHE_NAME)
                .setIndexedTypes(Integer.class, TestVal.class)
        );

        Map<Integer, TestVal> vals = new HashMap<>(VALS_CNT);

        for (int i = 0; i < VALS_CNT; i++)
            vals.put(i, new TestVal());

        cache.putAll(vals);

        CountingCdcConsumer cnsmr = new CountingCdcConsumer();

        CdcMain cdc = createCdc(cnsmr, getConfiguration(srv.name()));

        IgniteInternalFuture<Object> fut = runAsync(cdc);

        assertTrue(waitForCondition(() -> cnsmr.commited && cnsmr.evtsCnt == VALS_CNT, getTestTimeout()));

        waitForWalSegmentsHandling(srv, cdc);

        cnsmr.reset();

        forceRebuildIndexes(srv, srv.cachex(DEFAULT_CACHE_NAME).context());

        indexRebuildFuture(srv, cacheId(DEFAULT_CACHE_NAME)).get(getTestTimeout());

        waitForWalSegmentsHandling(srv, cdc);

        fut.cancel();
    }

    /** @param cdc Cdc. */
    private void waitForWalSegmentsHandling(IgniteEx srv, CdcMain cdc) throws IgniteInterruptedCheckedException {
        assertTrue("Wal segments was not committed by CdcConsumer", waitForCondition(() -> {
            long lastArchivedSeg = srv.context().cache().context().wal().lastArchivedSegment();

            MetricRegistry mreg = getFieldValue(cdc, "mreg");

            long committedCdcSegIdx = mreg.<LongMetric>findMetric(COMMITTED_SEG_IDX).value();
            long curCdcSegIdx = mreg.<LongMetric>findMetric(CUR_SEG_IDX).value();

            log.warning(String.format(">>>>>> Information about CDC and WAL: " +
                    "[lastArchivedSeg=%d, committedCdcSegIdx=%d, curCdcSegIdx=%d]",
                lastArchivedSeg,
                committedCdcSegIdx,
                curCdcSegIdx));

            return lastArchivedSeg == committedCdcSegIdx;
        }, WAL_ARCHIVE_TIMEOUT * 4, WAL_ARCHIVE_TIMEOUT / 2));
    }

    /** */
    public static class CountingCdcConsumer implements CdcConsumer {
        /** Commited. */
        volatile boolean commited;

        /** Evts count. */
        volatile int evtsCnt;

        /** */
        public void reset() {
            commited = false;
            evtsCnt = 0;
        }

        /** {@inheritDoc} */
        @Override public void start(MetricRegistry mreg) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean onEvents(Iterator<CdcEvent> events) {
            while (events.hasNext()) {
                events.next();

                evtsCnt++;
            }

            commited = true;

            return true;
        }

        /** {@inheritDoc} */
        @Override public void onTypes(Iterator<BinaryType> types) {
            types.forEachRemaining(t -> { /* No-op */ });
        }

        /** {@inheritDoc} */
        @Override public void onMappings(Iterator<TypeMapping> mappings) {
            mappings.forEachRemaining(m -> { /* No-op */ });
        }

        /** {@inheritDoc} */
        @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
            cacheEvents.forEachRemaining(e -> { /* No-op */ });
        }

        /** {@inheritDoc} */
        @Override public void onCacheDestroy(Iterator<Integer> caches) {
            caches.forEachRemaining(c -> { /* No-op */ });
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            // No-op.
        }
    }

    /** */
    public static class TestVal {
        /** Field 0. */
        @QuerySqlField(index = true, inlineSize = 256)
        private final String f0;

        /** Field 1. */
        @QuerySqlField(index = true, inlineSize = 256)
        private final String f1;

        /**
         * Default constructor.
         */
        public TestVal() {
            f0 = UUID.randomUUID().toString();
            f1 = UUID.randomUUID().toString();
        }
    }
}
