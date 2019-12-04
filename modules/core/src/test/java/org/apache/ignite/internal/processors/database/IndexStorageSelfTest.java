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

package org.apache.ignite.internal.processors.database;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorage;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.pagemem.PageIdUtils.MAX_PART_ID;

/**
 *
 */
public class IndexStorageSelfTest extends GridCommonAbstractTest {
    /** Make sure page is small enough to trigger multiple pages in a linked list. */
    private static final int PAGE_SIZE = 1024;

    /** */
    private static File allocationPath;

    /** */
    private static final char[] ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        allocationPath = U.resolveWorkDirectory(U.defaultWorkDirectory(), "pagemem", false);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testMetaIndexAllocation() throws Exception {
        metaAllocation();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testIndexRebuildMarkersSharedGroup() throws Exception {
        doTestIndexRebuildMarkers(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testIndexRebuildMarkers() throws Exception {
        doTestIndexRebuildMarkers(false);
    }

    /**
     *
     * @param grpShared Whether cache group is shared.
     * @throws Exception If failed.
     */
    public void doTestIndexRebuildMarkers(boolean grpShared) throws Exception {
        PageMemory mem = memory(true);

        mem.start();

        try {
            IndexStorage idxStore = createMetaStorage("group".hashCode(), mem, grpShared);

            log.info("Num pages=" + mem.loadedPages());

            List<Integer> parts = IntStream.range(0, MAX_PART_ID).boxed().collect(Collectors.toList());

            idxStore.storeIndexRebuildMarkers(DEFAULT_CACHE_NAME.hashCode(), parts, true);

            log.info("Num pages=" + mem.loadedPages());

            Collections.shuffle(parts);

            Set<Integer> partSet = new HashSet<>(parts);

            int cnt = parts.size();

            for (int idx = 0; idx < parts.size(); idx += cnt) {
                cnt = Math.max(1, cnt / 2);

                List<Integer> subList = parts.subList(idx, idx + cnt);

                partSet.removeAll(subList);

                idxStore.storeIndexRebuildMarkers(DEFAULT_CACHE_NAME.hashCode(), subList, false);

                Set<Integer> res = idxStore.getIndexRebuildMarkers(DEFAULT_CACHE_NAME.hashCode());

                assertEquals(partSet.size(), res.size());

                assertTrue(F.eqNotOrdered(partSet, res));
            }

            assertTrue(F.isEmpty(idxStore.getIndexRebuildMarkers(DEFAULT_CACHE_NAME.hashCode())));
        }
        finally {
            mem.stop(true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void metaAllocation() throws Exception {
        PageMemory mem = memory(true);

        int[] cacheIds = new int[]{1, "partitioned".hashCode(), "replicated".hashCode()};

        Map<Integer, Map<String, RootPage>> allocatedIdxs = new HashMap<>();

        mem.start();

        try {
            final Map<Integer, IndexStorage> storeMap = new HashMap<>();

            for (int i = 0; i < 1_000; i++) {
                int cacheId = cacheIds[i % cacheIds.length];

                Map<String, RootPage> idxMap = allocatedIdxs.get(cacheId);

                if (idxMap == null) {
                    idxMap = new HashMap<>();

                    allocatedIdxs.put(cacheId, idxMap);
                }

                String idxName;

                do {
                    idxName = randomName();
                } while (idxMap.containsKey(idxName));

                IndexStorage metaStore = storeMap.get(cacheId);

                if (metaStore == null) {
                    metaStore = createMetaStorage(cacheId, mem, false);

                    storeMap.put(cacheId, metaStore);
                }

                final RootPage rootPage = metaStore.allocateIndex(idxName);

                assertTrue(rootPage.isAllocated());

                idxMap.put(idxName, rootPage);
            }

            for (int cacheId : cacheIds) {
                Map<String, RootPage> idxMap = allocatedIdxs.get(cacheId);

                for (Map.Entry<String, RootPage> entry : idxMap.entrySet()) {
                    String idxName = entry.getKey();
                    FullPageId rootPageId = entry.getValue().pageId();

                    final RootPage rootPage = storeMap.get(cacheId).allocateIndex(idxName);

                    assertEquals("Invalid root page ID restored [cacheId=" + cacheId + ", idxName=" + idxName + ']',
                        rootPageId, rootPage.pageId());

                    assertFalse("Root page already allocated [cacheId=" + cacheId + ", idxName=" + idxName + ']',
                        rootPage.isAllocated());
                }
            }
        }
        finally {
            mem.stop(true);
        }
    }

    /**
     *
     * @param cacheId Cache ID.
     * @param mem Page memory.
     * @return Index storage.
     * @throws Exception If failed.
     */
    private IndexStorage createMetaStorage(int cacheId, PageMemory mem, boolean grpShared) throws Exception {
        return new IndexStorageImpl(
            mem,
            null,
            new AtomicLong(),
            cacheId,
            grpShared,
            PageIdAllocator.INDEX_PARTITION,
            PageMemory.FLAG_IDX,
            null,
            mem.allocatePage(cacheId, PageIdAllocator.INDEX_PARTITION, PageMemory.FLAG_IDX),
            true,
            mem.allocatePage(cacheId, PageIdAllocator.INDEX_PARTITION, PageMemory.FLAG_IDX),
            true,
            null,
            null,
            null
        );
    }

    /**
     * @return Random name.
     */
    private static String randomName() {
        StringBuilder sb = new StringBuilder();

        Random rnd = ThreadLocalRandom.current();

        int size = rnd.nextInt(25) + 1;

        for (int i = 0; i < size; i++)
            sb.append(ALPHABET[rnd.nextInt(ALPHABET.length)]);

        return sb.toString();
    }

    /**
     * @param clean Clean flag. If {@code true}, will clean previous memory state and allocate
     *      new empty page memory.
     * @return Page memory instance.
     */
    protected PageMemory memory(boolean clean) throws Exception {
        DirectMemoryProvider provider = new MappedFileMemoryProvider(log(), allocationPath);

        DataRegionConfiguration plcCfg = new DataRegionConfiguration()
            .setMaxSize(30L * 1024 * 1024).setInitialSize(30L * 1024 * 1024);

        return new PageMemoryNoStoreImpl(
            log,
            provider,
            null,
            PAGE_SIZE,
            plcCfg,
            new LongAdderMetric("NO_OP", null),
            true);
    }
}
