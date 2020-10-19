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

package org.apache.ignite.cdc;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class IgniteCDCSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        int segmentSz = 10 * 1024 * 1024;

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setCdcEnabled(true)
            .setWalMode(WALMode.FSYNC)
            .setMaxWalArchiveSize(10 * segmentSz)
            .setWalSegmentSize(segmentSz)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** Simplest CDC test. */
    @Test
    public void testReadAllKeys() throws Exception {
        StoreKeysCDCConsumer consumer = new StoreKeysCDCConsumer();

        IgniteCDC cdc = new IgniteCDC(getConfiguration("cdc"), consumer);

        try {
            runAsync(() -> {
                cdc.start();

                try {
                    cdc.join();
                }
                catch (InterruptedException ignore) {
                    // No-op.
                }
            });

            Ignite ign = startGrid();

            ign.cluster().state(ACTIVE);

            String cacheName = "my-cache";

            IgniteCache<Integer, byte[]> cache = ign.createCache(cacheName);

            int keysCnt = 10_000;

            for (int i = 0; i < keysCnt; i++) {
                byte[] bytes = new byte[1024];

                ThreadLocalRandom.current().nextBytes(bytes);

                cache.put(i, bytes);
            }

            boolean res = waitForCondition(() -> {
                Set<Integer> keys = consumer.keys(cacheId(cacheName));

                return F.size(keys) == keysCnt;
            }, 10_000);

            assertTrue(res);
        }
        finally {
            cdc.interrupt();

            cdc.join();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    private static class StoreKeysCDCConsumer implements CDCConsumer {
        /** Keys */
        private ConcurrentMap<Integer, Set> cacheKeys;

        /** {@inheritDoc} */
        @Override public String id() {
            return getClass().getName();
        }

        /** {@inheritDoc} */
        @Override public void start(IgniteConfiguration configuration, IgniteLogger log) {
            cacheKeys = new ConcurrentHashMap<>();
        }

        /** {@inheritDoc} */
        @Override public <T extends WALRecord> void onRecord(T record) {
            if (record.type() != WALRecord.RecordType.DATA_RECORD)
                return;

            DataRecord dataRecord = (DataRecord)record;

            for (DataEntry entry : dataRecord.writeEntries()) {
                cacheKeys.computeIfAbsent(entry.cacheId(), key -> new GridConcurrentHashSet());

                cacheKeys.get(entry.cacheId()).add(entry.key());
            }
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            // No-op.
        }

        /** @return Read keys. */
        public <K> Set<K> keys(int cacheId) {
            if (cacheKeys == null)
                return null;

            return (Set<K>)cacheKeys.get(cacheId);
        }
    }
}
