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

package org.apache.ignite.internal.portable;

import org.apache.ignite.internal.util.GridUnsafe;
import org.eclipse.jetty.util.ConcurrentHashSet;
import sun.misc.Unsafe;

/**
 * Field tests for heap-based portables.
 */
public class BinaryFieldsOffheapSelfTest extends BinaryFieldsAbstractSelfTest {
    /** Unsafe instance. */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** Byte array offset for unsafe mechanics. */
    protected static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** Allocated unsafe pointer. */
    private final ConcurrentHashSet<Long> ptrs = new ConcurrentHashSet<>();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        // Cleanup allocated objects.
        for (Long ptr : ptrs)
            UNSAFE.freeMemory(ptr);

        ptrs.clear();
    }

    /** {@inheritDoc} */
    @Override protected BinaryObjectExImpl toPortable(BinaryMarshaller marsh, Object obj) throws Exception {
        byte[] arr = marsh.marshal(obj);

        long ptr = UNSAFE.allocateMemory(arr.length);

        ptrs.add(ptr);

        UNSAFE.copyMemory(arr, BYTE_ARR_OFF, null, ptr, arr.length);

        return new BinaryObjectOffheapImpl(portableContext(marsh), ptr, 0, arr.length);
    }
}
