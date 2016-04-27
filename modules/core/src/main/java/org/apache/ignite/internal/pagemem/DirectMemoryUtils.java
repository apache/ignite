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

package org.apache.ignite.internal.pagemem;

import org.apache.ignite.internal.util.GridUnsafe;
import sun.misc.Unsafe;

/**
 *
 */
public class DirectMemoryUtils {
    /**
     * Reads long value from the given memory pointer.
     *
     * @param ptr Pointer to read.
     * @return Read value.
     */
    public long readLong(long ptr) {
        return GridUnsafe.getLong(ptr);
    }

    /**
     * Writes long value to the given memory pointer.
     *
     * @param ptr Pointer to write to.
     * @param val Value to write.
     */
    public void writeLong(long ptr, long val) {
        GridUnsafe.putLong(ptr, val);
    }

    /**
     * Reads integer value from the given memory pointer.
     *
     * @param ptr Pointer to read value from.
     * @return Read value.
     */
    public int readInt(long ptr) {
        return GridUnsafe.getInt(ptr);
    }

    /**
     * Writes integer value to the given memory pointer.
     *
     * @param ptr Pointer to write value to.
     * @param val Value to write.
     */
    public void writeInt(long ptr, int val) {
        GridUnsafe.putInt(ptr, val);
    }

    /**
     * Sets the given memory region with the given byte value.
     *
     * @param ptr Pointer to start filling from.
     * @param len Memory size to fill, in bytes.
     * @param val Value to set.
     */
    public void setMemory(long ptr, long len, byte val) {
        GridUnsafe.setMemory(ptr, len, val);
    }

    /**
     * CAS.
     *
     * @param ptr Memory pointer.
     * @param expVal Expected value.
     * @param newVal New value.
     * @return True if CAS succeeded.
     */
    public boolean compareAndSwapLong(long ptr, long expVal, long newVal) {
        return GridUnsafe.compareAndSwapLong(null, ptr, expVal, newVal);
    }

    /**
     * Volatile write long to the given memory pointer.
     *
     * @param ptr Memory pointer.
     * @param val Value to write.
     */
    public void writeLongVolatile(final long ptr, final long val) {
        GridUnsafe.putLongVolatile(null, ptr, val);
    }
}
