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

package org.apache.ignite.internal.pagemem.impl;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.util.GridDebug;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 * Base scenario for memory leak:
 * 1. Start topology with client nodes
 * 2. Call active(true) for cluster. This activation should fail by some circumstances (e.g. some locks exists)
 * 3. IgniteCacheDatabaseSharedManager started and onActive called here. Memory allocated.
 * 4. Call active(true) again. Activation successfull, non heap memory leak introduced.
 *
 */
public class PageMemoryNoStoreLeakTest extends GridCommonAbstractTest {
    /** */
    protected static final int PAGE_SIZE = 10 * 1024 * 1024;

    /** */
    private static final int MAX_MEMORY_SIZE = 100 * 1024 * 1024;

    /**
     * @throws Exception If failed.
     */
    public void testPageDoubleInitMemoryLeak() throws Exception {
        System.out.println("first vm size=" + GridDebug.getCommittedVirtualMemorySize() / (1024 * 1024));

        for (int i = 0; i < 10_000; i++) {
            final DirectMemoryProvider provider = new UnsafeMemoryProvider(log());

            final DirectMemoryRegion[] regions = {null};

            PageMemory mem =
                memory(new DirectMemoryProvider() {
                    @Override public void initialize(long[] chunkSizes) {
                        provider.initialize(chunkSizes);
                    }

                    @Override public void shutdown() {
                        provider.shutdown();
                    }

                    @Override public DirectMemoryRegion nextRegion() {
                        regions[0] = provider.nextRegion();
                        return regions[0];
                    }
                }, PAGE_SIZE);

            try {
                mem.start();

                // Fill allocated memory random bytes
                for (int j = 0; j < PAGE_SIZE; j++)
                    PageUtils.putByte(regions[0].address(), j, (byte)255);

                //Second initialization, introduces leak
                mem.start();
            }
            finally {
                mem.stop();
            }

            if(i % 50 == 0)
                System.out.println("last vm size=" + GridDebug.getCommittedVirtualMemorySize() / (1024 * 1024));
        }
    }

    /**
     * @param provider Memory provider e.g {@link UnsafeMemoryProvider}
     * @param pageSize Page size.
     * @return Page memory implementation.
     */
    protected PageMemory memory(DirectMemoryProvider provider, int pageSize) {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration()
            .setMaxSize(MAX_MEMORY_SIZE).setInitialSize(MAX_MEMORY_SIZE);

        return new PageMemoryNoStoreImpl(
            log(),
            provider,
            null,
            pageSize,
            plcCfg,
            new DataRegionMetricsImpl(plcCfg),
            true);
    }
}
