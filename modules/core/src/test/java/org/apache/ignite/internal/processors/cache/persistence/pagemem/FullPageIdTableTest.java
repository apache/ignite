/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.FullPageIdTable;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class FullPageIdTableTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_ID_RANGE = 10;

    /** */
    private static final int PAGE_ID_RANGE = 1000;

    /**
     * @throws Exception if failed.
     */
    public void testRandomOperations() throws Exception {
        long mem = FullPageIdTable.requiredMemory(CACHE_ID_RANGE * PAGE_ID_RANGE);

        UnsafeMemoryProvider prov = new UnsafeMemoryProvider(new JavaLogger());

        prov.initialize(new long[] {mem});

        DirectMemoryRegion region = prov.nextRegion();

        try {
            long seed = U.currentTimeMillis();

            info("Seed: " + seed + "L; //");

            Random rnd = new Random(seed);

            FullPageIdTable tbl = new FullPageIdTable(region.address(), region.size(), true);

            Map<FullPageId, Long> check = new HashMap<>();

            for (int i = 0; i < 10_000; i++) {
                int cacheId = rnd.nextInt(CACHE_ID_RANGE) + 1;
                int pageId = rnd.nextInt(PAGE_ID_RANGE);

                FullPageId fullId = new FullPageId(pageId, cacheId);

                boolean put = rnd.nextInt(3) != -1;

                if (put) {
                    long val = rnd.nextLong();

                    tbl.put(cacheId, pageId, val, 0);
                    check.put(fullId, val);
                }
                else {
                    tbl.remove(cacheId, pageId, 0);
                    check.remove(fullId);
                }

                verifyLinear(tbl, check);

                if (i > 0 && i % 1000 == 0)
                    info("Done: " + i);
            }
        }
        finally {
            prov.shutdown();
        }
    }

    /**
     * @param tbl Table to check.
     * @param check Expected mapping.
     */
    private void verifyLinear(FullPageIdTable tbl, Map<FullPageId, Long> check) {
        final Map<FullPageId, Long> collector = new HashMap<>();

        tbl.visitAll(new CI2<FullPageId, Long>() {
            @Override public void apply(FullPageId fullId, Long val) {
                if (collector.put(fullId, val) != null)
                    throw new AssertionError("Duplicate full page ID mapping: " + fullId);
            }
        });

        assertEquals("Size check failed", check.size(), collector.size());

        for (Map.Entry<FullPageId, Long> entry : check.entrySet())
            assertEquals("Mapping comparison failed for key: " + entry.getKey(),
                entry.getValue(), collector.get(entry.getKey()));
    }
}
