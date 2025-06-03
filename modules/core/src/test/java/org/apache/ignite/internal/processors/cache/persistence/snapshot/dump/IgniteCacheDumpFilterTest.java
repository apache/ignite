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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
import org.apache.ignite.IgniteException;
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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.dump.DumpReaderConfiguration.DFLT_THREAD_CNT;
import static org.apache.ignite.dump.DumpReaderConfiguration.DFLT_TIMEOUT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public class IgniteCacheDumpFilterTest extends GridCommonAbstractTest {
    /** */
    public static final Map<String, String> CACHE_TO_GRP = new HashMap<>();

    /** */
    private final Map<Integer, Integer> cacheSzs = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        CACHE_TO_GRP.put("c0", "g");
        CACHE_TO_GRP.put("c1", "g");
        CACHE_TO_GRP.put("c2", "g");
        CACHE_TO_GRP.put("mycache", null);
        CACHE_TO_GRP.put("mycache2", null);

        super.beforeTest();
    }

    /** */
    @Test
    public void testReadDumpWithCacheFilter() throws Exception {
        doTest(1);

        cacheSzs.clear();

        cleanPersistenceDir();

        doTest(3);
    }

    /** */
    private void doTest(int nodeCnt) throws Exception {
        try (IgniteEx srv = startGrids(nodeCnt)) {
            CACHE_TO_GRP.forEach((name, grp) -> createAndPutData(srv, name, grp, true));

            srv.snapshot().createDump("dump", null).get();
        }
        finally {
            stopAllGrids();
        }

        try (IgniteEx srv = startGrids(nodeCnt)) {
            SnapshotFileTree sft = new SnapshotFileTree(srv.context(), "dump", null);

            readAndCheck(srv, sft, null, "c1");
            readAndCheck(srv, sft, null, "c0", "c2");
            readAndCheck(srv, sft, null, "c0", "c1", "c2");
            readAndCheck(srv, sft, null, "c0", "mycache");
            readAndCheck(srv, sft, null, "c1", "mycache", "mycache2");
            readAndCheck(srv, sft, null, "mycache", "mycache2");

            readAndCheck(srv, sft, Set.of("g"), "c1");
            readAndCheck(srv, sft, Set.of("g"), "c0", "mycache");
            readAndCheck(srv, sft, Set.of("g"), "mycache", "mycache2");

            corruptFileThatMustBeSkiped(srv, sft);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Read dump similar to {@link #readAndCheck(IgniteEx, SnapshotFileTree, Set, String...) but corrupt all other files, first.
     * Check that files not read during dump read.
     */
    private void corruptFileThatMustBeSkiped(IgniteEx srv, SnapshotFileTree sft) throws Exception {
        corruptAllPartitions(sft, config("mycache", null));

        assertThrowsWithCause(() -> readAndCheck(srv, sft, null, "c1", "mycache"), IgniteException.class);

        srv.cacheNames().forEach(srv::destroyCache);

        awaitPartitionMapExchange();

        // Will succeed only if all other files, except for "mycache2" will be not read.
        readAndCheck(srv, sft, null, "c1", "mycache2");

        corruptAllPartitions(sft, config("c0", "g"));

        readAndCheck(srv, sft, null, "mycache2");
    }

    /** */
    private void readAndCheck(IgniteEx srv, SnapshotFileTree sft, Set<String> grpNames, String...cacheNames) {
        assertTrue(cacheNames != null);
        assertTrue(cacheNames.length > 0);

        Set<Integer> cacheIds = Arrays.stream(cacheNames)
            .filter(name -> grpNames == null || grpNames.contains(groupName(name)))
            .map(CU::cacheId).collect(Collectors.toSet());

        FilterCacheConsumer cnsmr = new FilterCacheConsumer(srv, cacheIds);

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
                grpNames == null ? null : grpNames.toArray(U.EMPTY_STRS),
                cacheNames,
                true,
                null
            ),
            log
        ).run();

        int ecnt = 0;

        for (String name : cacheNames) {
            if (grpNames != null && !grpNames.contains(groupName(name))) {
                assertFalse(srv.cacheNames().contains(name));
                continue;
            }

            IgniteCache<Integer, Integer> cache = srv.cache(name);

            int expSz = cacheSzs.get(CU.cacheId(name));

            ecnt += expSz;

            assertEquals(expSz, cache.size());

            IntStream.range(0, expSz).forEach(i -> assertEquals((Integer)i, cache.get(i)));

            srv.destroyCache(name);
        }

        try {
            awaitPartitionMapExchange();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertEquals(ecnt, cnsmr.entryCnt);
        assertTrue(srv.cacheNames().isEmpty());
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

    /** */
    private static void corruptAllPartitions(SnapshotFileTree sft, CacheConfiguration<?, ?> ccfg) throws IOException {
        for (File partFile : sft.existingCachePartitionFiles(sft.cacheStorage(ccfg), true, false)) {
            try (FileOutputStream fos = new FileOutputStream(partFile)) {
                // Corrupt file by writing zeroes to it.
                fos.write(new byte[200]);
            }
        }
    }

    /** */
    private static class FilterCacheConsumer implements DumpConsumer {
        /** */
        private final IgniteEx srv;

        /** */
        private final Set<Integer> cacheIds;

        /** */
        int entryCnt;

        /** */
        public FilterCacheConsumer(IgniteEx srv, Set<Integer> cacheIds) {
            this.srv = srv;
            this.cacheIds = cacheIds;
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
            if (cacheIds.isEmpty()) {
                assertFalse(caches.hasNext());
                return;
            }

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

    /** */
    public static String groupName(String name) {
        String grpName = CACHE_TO_GRP.get(name);
        return grpName == null ? name : grpName;
    }
}
