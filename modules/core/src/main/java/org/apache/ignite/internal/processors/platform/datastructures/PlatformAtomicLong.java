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

package org.apache.ignite.internal.processors.platform.datastructures;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicLongImpl;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;

/**
 * Platform atomic long wrapper.
 */
public class PlatformAtomicLong extends PlatformAbstractTarget {
    /** */
    private final GridCacheAtomicLongImpl atomicLong;

    /** */
    private static final int OP_ADD_AND_GET = 1;

    /** */
    private static final int OP_CLOSE = 2;

    /** */
    private static final int OP_COMPARE_AND_SET = 3;

    /** */
    private static final int OP_COMPARE_AND_SET_AND_GET = 4;

    /** */
    private static final int OP_DECREMENT_AND_GET = 5;

    /** */
    private static final int OP_GET = 6;

    /** */
    private static final int OP_GET_AND_ADD = 7;

    /** */
    private static final int OP_GET_AND_DECREMENT = 8;

    /** */
    private static final int OP_GET_AND_INCREMENT = 9;

    /** */
    private static final int OP_GET_AND_SET = 10;

    /** */
    private static final int OP_INCREMENT_AND_GET = 11;

    /** */
    private static final int OP_IS_CLOSED = 12;

    /**
     * Ctor.
     * @param ctx Context.
     * @param atomicLong AtomicLong to wrap.
     */
    public PlatformAtomicLong(PlatformContext ctx, GridCacheAtomicLongImpl atomicLong) {
        super(ctx);

        assert atomicLong != null;

        this.atomicLong = atomicLong;
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        switch (type) {
            case OP_COMPARE_AND_SET:
                long cmp = reader.readLong();
                long val = reader.readLong();

                return atomicLong.compareAndSet(cmp, val) ? TRUE : FALSE;

            case OP_COMPARE_AND_SET_AND_GET:
                long expVal = reader.readLong();
                long newVal = reader.readLong();

                return atomicLong.compareAndSetAndGet(expVal, newVal);
        }

        return super.processInStreamOutLong(type, reader);
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_ADD_AND_GET:
                return atomicLong.addAndGet(val);

            case OP_GET_AND_ADD:
                return atomicLong.getAndAdd(val);

            case OP_GET_AND_SET:
                return atomicLong.getAndSet(val);

            case OP_CLOSE:
                atomicLong.close();

                return TRUE;

            case OP_DECREMENT_AND_GET:
                return atomicLong.decrementAndGet();

            case OP_GET:
                return atomicLong.get();

            case OP_GET_AND_DECREMENT:
                return atomicLong.getAndDecrement();

            case OP_GET_AND_INCREMENT:
                return atomicLong.getAndIncrement();

            case OP_INCREMENT_AND_GET:
                return atomicLong.incrementAndGet();

            case OP_IS_CLOSED:
                return atomicLong.removed() ? TRUE : FALSE;
        }

        return super.processInLongOutLong(type, val);
    }
}
