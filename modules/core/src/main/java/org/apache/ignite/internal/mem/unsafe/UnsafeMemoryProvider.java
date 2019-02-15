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

package org.apache.ignite.internal.mem.unsafe;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.UnsafeChunk;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Memory provider implementation based on unsafe memory access.
 * <p>
 * Supports memory reuse semantics.
 */
public class UnsafeMemoryProvider implements DirectMemoryProvider {
    /** */
    private long[] sizes;

    /** */
    private List<DirectMemoryRegion> regions;

    /** */
    private IgniteLogger log;

    /** Flag shows if current memory provider have been already initialized. */
    private boolean isInit;

    /** */
    private int used = 0;

    /**
     * @param log Ignite logger to use.
     */
    public UnsafeMemoryProvider(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void initialize(long[] sizes) {
        if (isInit)
            return;

        this.sizes = sizes;

        regions = new ArrayList<>();

        isInit = true;
    }

    /** {@inheritDoc} */
    @Override public void shutdown(boolean deallocate) {
        if (regions != null) {
            for (Iterator<DirectMemoryRegion> it = regions.iterator(); it.hasNext(); ) {
                DirectMemoryRegion chunk = it.next();

                if (deallocate) {
                    GridUnsafe.freeMemory(chunk.address());

                    // Safety.
                    it.remove();
                }
            }

            if (!deallocate)
                used = 0;
        }
    }

    /** {@inheritDoc} */
    @Override public DirectMemoryRegion nextRegion() {
        if (used == sizes.length)
            return null;

        if (used < regions.size())
            return regions.get(used++);

        long chunkSize = sizes[regions.size()];

        long ptr;

        try {
            ptr = GridUnsafe.allocateMemory(chunkSize);
        }
        catch (IllegalArgumentException e) {
            String msg = "Failed to allocate next memory chunk: " + U.readableSize(chunkSize, true) +
                ". Check if chunkSize is too large and 32-bit JVM is used.";

            if (regions.isEmpty())
                throw new IgniteException(msg, e);

            U.error(log, msg);

            return null;
        }

        if (ptr <= 0) {
            U.error(log, "Failed to allocate next memory chunk: " + U.readableSize(chunkSize, true));

            return null;
        }

        DirectMemoryRegion region = new UnsafeChunk(ptr, chunkSize);

        regions.add(region);

        used++;

        return region;
    }
}
