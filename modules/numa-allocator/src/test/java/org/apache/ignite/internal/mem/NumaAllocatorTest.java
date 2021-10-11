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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class NumaAllocatorTest extends GridCommonAbstractTest {
    /** */
    private static final long BUF_SZ = 32 * 1024 * 1024;

    /** */
    @Parameterized.Parameters(name = "allocationStrategy={0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[]{ new LocalNumaAllocationStrategy() },
            new Object[]{ new InterleavedNumaAllocationStrategy() },
            new Object[]{ new InterleavedNumaAllocationStrategy(IntStream.of(0, 2).toArray()) },
            new Object[]{ new SimpleNumaAllocationStrategy() },
            new Object[]{ new SimpleNumaAllocationStrategy(2) }
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

            GridUnsafe.setMemory(ptr, BUF_SZ, (byte)1);

//            for (long i = 0; i < BUF_SZ; i++) {
//                assertEquals((byte)1, GridUnsafe.getByte(ptr + i));
//            }
        }
        finally {
            if (ptr != 0) {
                allocator.freeMemory(ptr);
            }
        }
    }
}
