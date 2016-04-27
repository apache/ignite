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

import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.database.MetadataStorage;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class MetadataStorageSelfTest extends GridCommonAbstractTest {
    /** Make sure page is small enough to trigger multiple pages in a linked list. */
    public static final int PAGE_SIZE = 1024;

    /** */
    private static File allocationPath;

    /** */
    private static final char[] ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        allocationPath = U.resolveWorkDirectory("pagemem", false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testMetaIndexAllocation() throws Exception {
        PageMemory mem = memory(true);

        int[] cacheIds = new int[]{1, "partitioned".hashCode(), "replicated".hashCode()};

        Map<Integer, Map<String, FullPageId>> allocatedIdxs = new HashMap<>();

        mem.start();

        try {
            MetadataStorage metaStore = new MetadataStorage(mem);

            for (int i = 0; i < 1_000; i++) {
                int cacheId = cacheIds[i % cacheIds.length];

                Map<String, FullPageId> idxMap = allocatedIdxs.get(cacheId);

                if (idxMap == null) {
                    idxMap = new HashMap<>();

                    allocatedIdxs.put(cacheId, idxMap);
                }

                String idxName;

                do {
                    idxName = randomName();
                } while (idxMap.containsKey(idxName));

                IgniteBiTuple<FullPageId, Boolean> rootPageId = metaStore.getOrAllocateForIndex(cacheId, idxName);

                assertTrue(rootPageId.get2());

                idxMap.put(idxName, rootPageId.get1());
            }

            for (int cacheId : cacheIds) {
                Map<String, FullPageId> idxMap = allocatedIdxs.get(cacheId);

                for (Map.Entry<String, FullPageId> entry : idxMap.entrySet()) {
                    String idxName = entry.getKey();
                    FullPageId rootPageId = entry.getValue();

                    assertEquals("Invalid root page ID restored [cacheId=" + cacheId + ", idxName=" + idxName + ']',
                        rootPageId, metaStore.getOrAllocateForIndex(cacheId, idxName).get1());
                }
            }
        }
        finally {
            mem.stop();
        }

        mem = memory(false);

        mem.start();

        try {
            MetadataStorage meta = new MetadataStorage(mem);

            for (int cacheId : cacheIds) {
                Map<String, FullPageId> idxMap = allocatedIdxs.get(cacheId);

                for (Map.Entry<String, FullPageId> entry : idxMap.entrySet()) {
                    String idxName = entry.getKey();
                    FullPageId rootPageId = entry.getValue();

                    assertEquals("Invalid root page ID restored [cacheId=" + cacheId + ", idxName=" + idxName + ']',
                        rootPageId, meta.getOrAllocateForIndex(cacheId, idxName).get1());
                }
            }
        }
        finally {
            mem.stop();
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
    private PageMemory memory(boolean clean) {
        MappedFileMemoryProvider provider = new MappedFileMemoryProvider(log(), allocationPath, clean,
            20 * 1024 * 1024, 2 * 1024 * 1024);

        return new PageMemoryImpl(log, provider, null, PAGE_SIZE, Runtime.getRuntime().availableProcessors());
    }
}
