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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.dump.DumpConsumer;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.dump.DumpReader;
import org.apache.ignite.dump.DumpReaderConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.dump.DumpReaderConfiguration.DFLT_THREAD_CNT;
import static org.apache.ignite.dump.DumpReaderConfiguration.DFLT_TIMEOUT;

/** */
public class IgniteCacheDumpFilterTest extends GridCommonAbstractTest {
    /** */
    private final Map<Integer, Integer> cacheSzs = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** */
    @Test
    public void testReadDumpWithCacheFilter() throws Exception {
        try (IgniteEx srv = startGrid(1)) {
            createCaches(srv, true);

            srv.snapshot().createDump("dump", null).get();
        }

        try (IgniteEx srv = startGrid(1)) {
            SnapshotFileTree sft = new SnapshotFileTree(srv.context(), "dump", null);

            readAndCheck(srv, sft, "c1");
            readAndCheck(srv, sft, "c0", "c2");
            readAndCheck(srv, sft, "c0", "c1", "c2");
            readAndCheck(srv, sft, "c0", "mycache");
            readAndCheck(srv, sft, "c1", "mycache", "mycache2");
            readAndCheck(srv, sft, "mycache", "mycache2");
            readAndCheck(srv, sft, "mycache2");
        }
    }

    /** */
    private void readAndCheck(IgniteEx srv, SnapshotFileTree sft, String... cacheNames) throws Exception {
        assertTrue(cacheNames != null);
        assertTrue(cacheNames.length > 0);

        FilterCacheConsumer cnsmr = new FilterCacheConsumer(srv, Arrays.stream(cacheNames).map(CU::cacheId).collect(Collectors.toSet()));

        new DumpReader(
            new DumpReaderConfiguration(
                sft.name(),
                sft.path(),
                srv.configuration(),
                cnsmr,
                DFLT_THREAD_CNT,
                DFLT_TIMEOUT,
                true,
                false,
                false,
                null,
                cacheNames,
                true,
                null
            ),
            log
        ).run();

        int ecnt = 0;

        for (String name : cacheNames) {
            IgniteCache<Integer, Integer> cache = srv.cache(name);

            int expSz = cacheSzs.get(CU.cacheId(name));

            ecnt += expSz;

            assertEquals(expSz, cache.size());

            IntStream.range(0, expSz).forEach(i -> assertEquals((Integer)i, cache.get(i)));

            srv.destroyCache(name);
        }

        awaitPartitionMapExchange();

        assertEquals(ecnt, cnsmr.entryCnt);
        assertTrue(srv.cacheNames().isEmpty());
    }

    /** */
    private void createCaches(IgniteEx srv, boolean putData) {
        for (int i = 0; i < 3; i++)
            createAndPutData(srv, "c" + i, "g", putData);

        createAndPutData(srv, "mycache", null, putData);

        createAndPutData(srv, "mycache2", null, putData);
    }

    /** */
    private void createAndPutData(IgniteEx srv, String name, String grp, boolean putData) {
        IgniteCache<Integer, Integer> c = srv.createCache(config(name, grp));

        if (putData) {
            int cacheId = CU.cacheId(name);
            int cacheSz = ThreadLocalRandom.current().nextInt(100);

            assertNull(cacheSzs.put(cacheId, cacheSz));

            IntStream.range(0, cacheSz).forEach(idx -> c.put(idx, idx));
        }

    }

    /** */
    private CacheConfiguration<Integer, Integer> config(String name, String grp) {
        return new CacheConfiguration<Integer, Integer>(name)
            .setGroupName(grp)
            .setAffinity(new RendezvousAffinityFunction(10, null));
    }

    private class FilterCacheConsumer implements DumpConsumer {
        /** */
        private final IgniteEx srv;

        /** */
        private final Set<Integer> cacheIds;

        /** */
        int entryCnt;

        public FilterCacheConsumer(IgniteEx srv, Set<Integer> cacheIds) {
            this.srv = srv;
            this.cacheIds = cacheIds;

            assertFalse(cacheIds.isEmpty());
        }

        /** {@inheritDoc} */
        @Override public void start() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onMappings(Iterator<TypeMapping> mappings) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onTypes(Iterator<BinaryType> types) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onCacheConfigs(Iterator<StoredCacheData> caches) {
            Set<Integer> expCacheIds = new HashSet<>(cacheIds);

            caches.forEachRemaining(d -> {
                expCacheIds.remove(d.cacheId());

                srv.createCache(d.config());
            });

            assertTrue("Must find all caches", expCacheIds.isEmpty());
        }

        /** {@inheritDoc} */
        @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
            data.forEachRemaining(e -> {
                assertTrue(cacheIds.contains(e.cacheId()));

                entryCnt++;

                String name = srv.cacheNames().stream().filter(n -> CU.cacheId(n) == e.cacheId()).findFirst().orElseThrow();

                srv.cache(name).put(e.key(), e.value());
            });

        }

        /** {@inheritDoc} */
        @Override public void stop() {
            // No-op.
        }
    }
}