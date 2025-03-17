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

package org.apache.ignite.internal.mem;

import java.util.Arrays;
import java.util.stream.IntStream;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.mem.InterleavedNumaAllocationStrategy;
import org.apache.ignite.mem.LocalNumaAllocationStrategy;
import org.apache.ignite.mem.NumaAllocationStrategy;
import org.apache.ignite.mem.NumaAllocator;
import org.apache.ignite.mem.SimpleNumaAllocationStrategy;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Enclosed.class)
public class NumaAllocatorUnitTest {
    /** */
    @RunWith(Parameterized.class)
    public static class PositiveScenarioTest extends GridCommonAbstractTest {
        /** */
        private static final long BUF_SZ = 32 * 1024 * 1024;

        /** */
        private static final int[] EVEN_NODES = IntStream.range(0, NumaAllocUtil.NUMA_NODES_CNT)
            .filter(x -> x % 2 == 0).toArray();

        /** */
        private static final int[] ALL_NODES = IntStream.range(0, NumaAllocUtil.NUMA_NODES_CNT).toArray();

        /**
         *
         */
        @Parameterized.Parameters(name = "allocationStrategy={0}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(
                new Object[] {new LocalNumaAllocationStrategy()},
                new Object[] {new InterleavedNumaAllocationStrategy()},
                new Object[] {new InterleavedNumaAllocationStrategy(new int[0])},
                new Object[] {new InterleavedNumaAllocationStrategy(EVEN_NODES)},
                new Object[] {new InterleavedNumaAllocationStrategy(ALL_NODES)},
                new Object[] {new SimpleNumaAllocationStrategy()},
                new Object[] {new SimpleNumaAllocationStrategy(NumaAllocUtil.NUMA_NODES_CNT - 1)}
            );
        }

        /** */
        @Parameterized.Parameter()
        public NumaAllocationStrategy strategy;

        /** */
        @Test
        public void test() {
            NumaAllocator allocator = new NumaAllocator(strategy);

            long ptr = 0;
            try {
                ptr = allocator.allocateMemory(BUF_SZ);

                assertEquals(BUF_SZ, NumaAllocUtil.chunkSize(ptr));

                GridUnsafe.setMemory(ptr, BUF_SZ, (byte)1);

                for (long i = 0; i < BUF_SZ; i++)
                    assertEquals((byte)1, GridUnsafe.getByte(ptr + i));
            }
            finally {
                if (ptr != 0)
                    allocator.freeMemory(ptr);
            }
        }
    }

    /** */
    public static class ErrorScenarioTest extends GridCommonAbstractTest {
        /** */
        @Test
        public void testInvalidInterleavedStrategyParams() {
            int[][] invalidNodes = {
                {-3, -4, 0},
                IntStream.range(0, NumaAllocUtil.NUMA_NODES_CNT + 1).toArray()
            };

            for (int[] nodeSet: invalidNodes) {
                GridTestUtils.assertThrows(log(), () -> new InterleavedNumaAllocationStrategy(nodeSet),
                    IllegalArgumentException.class, null);
            }
        }

        /** */
        @Test
        public void testInvalidSimpleStrategyParams() {
            int[] invalidNodes = {-3, NumaAllocUtil.NUMA_NODES_CNT};

            for (int node: invalidNodes) {
                GridTestUtils.assertThrows(log(), () -> new SimpleNumaAllocationStrategy(node),
                    IllegalArgumentException.class, null);
            }
        }
    }
}
