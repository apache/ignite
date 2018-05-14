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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.util.GridDebug;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Base scenario for memory leak:
 * 1. Start topology with client nodes;
 * 2. Call active(true) for cluster. This activation should fail by some circumstances (e.g. some locks exists);
 * 3. IgniteCacheDatabaseSharedManager started and onActive called here. Memory allocated;
 * 4. Call active(true) again. Activation successfull, non heap memory leak introduced;
 */
public class PageMemoryNoStoreLeakTest extends GridCommonAbstractTest {
    /** */
    private static final int ONE_MEGABYTE = 1024 * 1024;

    /** */
    private static final int PAGE_SIZE = 10 * ONE_MEGABYTE;

    /** */
    private static final int MAX_MEMORY_SIZE = 100 * ONE_MEGABYTE;

    /** Allow 1Mb leaks. */
    private static final int ALLOW_LEAK_IN_MB = 1;

    /**
     * @throws Exception If failed.
     */
    public void testPageDoubleInitMemoryLeak() throws Exception {
        long startedVMSize = GridDebug.getCommittedVirtualMemorySize() / ONE_MEGABYTE;

        for (int i = 0; i < 1_000; i++) {
            final DirectMemoryProvider provider = new UnsafeMemoryProvider(log());

            final DirectMemoryRegion[] regions = {null};

            PageMemory mem = memory(new DirectMemoryProvider() {
                /** {@inheritDoc} */
                @Override public void initialize(long[] chunkSizes) {
                    provider.initialize(chunkSizes);
                }

                /** {@inheritDoc} */
                @Override public void shutdown() {
                    provider.shutdown();
                }

                /** {@inheritDoc} */
                @Override public DirectMemoryRegion nextRegion() {
                    regions[0] = provider.nextRegion();
                    return regions[0];
                }
            });

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

            long committedVMSize = GridDebug.getCommittedVirtualMemorySize() / ONE_MEGABYTE;

            assertTrue(committedVMSize - startedVMSize <= ALLOW_LEAK_IN_MB);
        }
    }

    /**
     * @param provider Memory provider e.g {@link UnsafeMemoryProvider}
     * @return Page memory implementation.
     */
    private PageMemory memory(DirectMemoryProvider provider) {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration()
            .setMaxSize(MAX_MEMORY_SIZE).setInitialSize(MAX_MEMORY_SIZE);

        return new PageMemoryNoStoreImpl(
            log(),
            provider,
            null,
            PAGE_SIZE,
            plcCfg,
            new DataRegionMetricsImpl(plcCfg),
            true);
    }
}
