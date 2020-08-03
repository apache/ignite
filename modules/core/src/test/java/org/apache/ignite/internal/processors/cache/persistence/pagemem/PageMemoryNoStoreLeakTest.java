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
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.D;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Base scenario for memory leak:
 * 1. Start topology with client nodes;
 * 2. Call active(true) for cluster. This activation should fail by some circumstances (e.g. some locks exists);
 * 3. IgniteCacheDatabaseSharedManager started and onActive called here. Memory allocated;
 * 4. Call active(true) again. Activation successfull, non heap memory leak introduced;
 */
public class PageMemoryNoStoreLeakTest extends GridCommonAbstractTest {
    /** */
    private static final int PAGE_SIZE = 4 * 1024;

    /** */
    private static final int MAX_MEMORY_SIZE = 10 * 1024 * 1024;

    /** Allow delta between GC executions. */
    private static final int ALLOWED_DELTA = 10 * 1024 * 1024;

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageDoubleInitMemoryLeak() throws Exception {
        long initVMsize = D.getCommittedVirtualMemorySize();

        for (int i = 0; i < 1_000; i++) {
            final DirectMemoryProvider provider = new UnsafeMemoryProvider(log());

            final DataRegionConfiguration plcCfg = new DataRegionConfiguration()
                .setMaxSize(MAX_MEMORY_SIZE).setInitialSize(MAX_MEMORY_SIZE);

            PageMemory mem = new PageMemoryNoStoreImpl(
                log(),
                provider,
                null,
                PAGE_SIZE,
                plcCfg,
                new LongAdderMetric("NO_OP", null),
                true);

            try {
                mem.start();

                //Second initialization, introduces leak
                mem.start();
            }
            finally {
                mem.stop(true);
            }

            long committedVMSize = D.getCommittedVirtualMemorySize();

            assertTrue(committedVMSize - initVMsize <= ALLOWED_DELTA);
        }
    }
}
