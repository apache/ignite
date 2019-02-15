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

import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;

/**
 * Platform atomic sequence wrapper.
 */
public class PlatformAtomicSequence extends PlatformAbstractTarget {
    /** */
    private final IgniteAtomicSequence atomicSeq;

    /** */
    private static final int OP_ADD_AND_GET = 1;

    /** */
    private static final int OP_CLOSE = 2;

    /** */
    private static final int OP_GET = 3;

    /** */
    private static final int OP_GET_AND_ADD = 4;

    /** */
    private static final int OP_GET_AND_INCREMENT = 5;

    /** */
    private static final int OP_GET_BATCH_SIZE = 6;

    /** */
    private static final int OP_INCREMENT_AND_GET = 7;

    /** */
    private static final int OP_IS_CLOSED = 8;

    /** */
    private static final int OP_SET_BATCH_SIZE = 9;

    /**
     * Ctor.
     * @param ctx Context.
     * @param atomicSeq AtomicSequence to wrap.
     */
    public PlatformAtomicSequence(PlatformContext ctx, IgniteAtomicSequence atomicSeq) {
        super(ctx);

        assert atomicSeq != null;

        this.atomicSeq = atomicSeq;
    }


    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_ADD_AND_GET:
                return atomicSeq.addAndGet(val);

            case OP_GET_AND_ADD:
                return atomicSeq.getAndAdd(val);

            case OP_SET_BATCH_SIZE:
                atomicSeq.batchSize((int)val);

                return TRUE;

            case OP_CLOSE:
                atomicSeq.close();

                return TRUE;

            case OP_GET:
                return atomicSeq.get();

            case OP_GET_AND_INCREMENT:
                return atomicSeq.getAndIncrement();

            case OP_INCREMENT_AND_GET:
                return atomicSeq.incrementAndGet();

            case OP_IS_CLOSED:
                return atomicSeq.removed() ? TRUE : FALSE;

            case OP_GET_BATCH_SIZE:
                return atomicSeq.batchSize();
        }

        return super.processInLongOutLong(type, val);
    }
}
