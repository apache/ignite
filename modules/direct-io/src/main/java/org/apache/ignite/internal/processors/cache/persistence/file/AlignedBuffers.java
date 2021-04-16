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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.nio.ByteBuffer;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Utility class for work with aligned buffers.
 */
@SuppressWarnings("WeakerAccess")
public class AlignedBuffers {
    /**
     * Allocates align memory for use with O_DIRECT and returns native byte buffer.
     * @param fsBlockSize alignment, FS ans OS block size.
     * @param size capacity.
     * @return byte buffer, to be released by {@link #free(ByteBuffer)}.
     */
    public static ByteBuffer allocate(int fsBlockSize, int size) {
        assert fsBlockSize > 0;
        assert size > 0;

        PointerByReference refToPtr = new PointerByReference();

        int retVal = IgniteNativeIoLib.posix_memalign(refToPtr, new NativeLong(fsBlockSize),
            new NativeLong(size));

        if (retVal != 0)
            throw new IgniteOutOfMemoryException("Failed to allocate memory: " + IgniteNativeIoLib.strerror(retVal));

        return GridUnsafe.wrapPointer(Pointer.nativeValue(refToPtr.getValue()), size);
    }

    /**
     * Frees the memory space used by direct buffer, which must have been returned by a previous call
     * {@link #allocate(int, int)}.
     *
     * @param buf direct buffer to free.
     */
    public static void free(ByteBuffer buf) {
        free(GridUnsafe.bufferAddress(buf));
    }

    /**
     * Frees the memory space pointed to by {@code addr} - address of buffer, which must have been returned by a
     * previous call {@link #allocate(int, int)}.
     *
     * @param addr direct buffer address to free.
     */
    public static void free(long addr) {
        IgniteNativeIoLib.free(new Pointer(addr));
    }
}
