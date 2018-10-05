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

package org.apache.ignite.internal.binary;

import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.GridUnsafe;
import org.eclipse.jetty.util.ConcurrentHashSet;

/**
 * Field tests for heap-based binaries.
 */
public class BinaryFieldsOffheapSelfTest extends BinaryFieldsAbstractSelfTest {
    /** Allocated unsafe pointer. */
    private final ConcurrentHashSet<Long> ptrs = new ConcurrentHashSet<>();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        // Cleanup allocated objects.
        for (Long ptr : ptrs)
            Ignition.GRID_UNSAFE.freeMemory(ptr);

        ptrs.clear();
    }

    /** {@inheritDoc} */
    @Override protected BinaryObjectExImpl toBinary(BinaryMarshaller marsh, Object obj) throws Exception {
        byte[] arr = marsh.marshal(obj);

        long ptr = Ignition.GRID_UNSAFE.allocateMemory(arr.length);

        ptrs.add(ptr);

        Ignition.GRID_UNSAFE.copyHeapOffheap(arr, Ignition.GRID_UNSAFE.BYTE_ARR_OFF, ptr, arr.length);

        return new BinaryObjectOffheapImpl(binaryContext(marsh), ptr, 0, arr.length);
    }
}
