/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence.file;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import java.nio.ByteBuffer;
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
