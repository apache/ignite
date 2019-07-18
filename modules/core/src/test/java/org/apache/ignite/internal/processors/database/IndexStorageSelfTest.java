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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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
     * @throws Exception If failed.
     */
    private void metaAllocation() throws Exception {
        PageMemory mem = memory(true);

        int[] cacheIds = new int[]{1, "partitioned".hashCode(), "replicated".hashCode()};

        Map<Integer, Map<String, RootPage>> allocatedIdxs = new HashMap<>();

        mem.start();

        try {
            final Map<Integer, IndexStorageImpl> storeMap = new HashMap<>();

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

                IndexStorageImpl metaStore = storeMap.get(cacheId);

                if (metaStore == null) {
                    metaStore = new IndexStorageImpl(
                        mem,
                        null,
                        new AtomicLong(),
                        cacheId,
                        false,
                        PageIdAllocator.INDEX_PARTITION,
                        PageMemory.FLAG_IDX,
                        null,
                        mem.allocatePage(cacheId, PageIdAllocator.INDEX_PARTITION, PageMemory.FLAG_IDX),
                        true,
                        null,
                        null
                    );

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
