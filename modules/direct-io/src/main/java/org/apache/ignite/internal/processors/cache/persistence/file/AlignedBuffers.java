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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.GridUnsafe;
import sun.nio.ch.DirectBuffer;

/**
 *
 */
public class AlignedBuffers {
    public static ByteBuffer allocate(int fsBlockSize, int capacity) {
        assert fsBlockSize > 0;
        assert capacity > 0;
        PointerByReference pointerToPointer = new PointerByReference();

        // align memory for use with O_DIRECT
        IgniteNativeIoLib.posix_memalign(pointerToPointer, new NativeLong(fsBlockSize), new NativeLong(capacity));
        Pointer pointer = pointerToPointer.getValue();
        long alignedPtr = Pointer.nativeValue(pointer);

        return GridUnsafe.wrapPointer(alignedPtr, capacity);
    }

    public static void free(ByteBuffer buffer) {
        free(GridUnsafe.bufferAddress(buffer));
    }

    public static void free(long address) {
        IgniteNativeIoLib.free(new Pointer(address));
    }

    static void copyMemory(ByteBuffer src, ByteBuffer dest) {
        //todo check bounds
        int size = src.remaining();

        if (src instanceof DirectBuffer && dest instanceof DirectBuffer) {
            GridUnsafe.copyMemory(GridUnsafe.bufferAddress(src), GridUnsafe.bufferAddress(dest), size);
        }
        else if (!(src instanceof DirectBuffer) && dest instanceof DirectBuffer) {
            new Pointer(GridUnsafe.bufferAddress(dest)).write(0, src.array(), src.arrayOffset(), size);
        }
        else if ((src instanceof DirectBuffer) && !(dest instanceof DirectBuffer)) {
            new Pointer(GridUnsafe.bufferAddress(src)).read(0, dest.array(), dest.arrayOffset(), size);
        }
        else {
            throw new UnsupportedOperationException("Unable to copy memory from [" + src.getClass() + "]" +
                " to [" + dest.getClass() + "]");
        }
    }
}
