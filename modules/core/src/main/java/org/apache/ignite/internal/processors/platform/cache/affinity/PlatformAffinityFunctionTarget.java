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

package org.apache.ignite.internal.processors.platform.cache.affinity;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.List;

/**
 * Platform affinity function target:
 * to be invoked when Platform function calls base implementation of one of the AffinityFunction methods.
 */
public class PlatformAffinityFunctionTarget extends PlatformAbstractTarget {
    /** */
    private static final int OP_PARTITION = 1;

    /** */
    private static final int OP_REMOVE_NODE = 2;

    /** */
    private static final int OP_ASSIGN_PARTITIONS = 3;

    /** Inner function to delegate calls to. */
    private final AffinityFunction baseFunc;

    /** Thread local to hold the current affinity function context. */
    private static final ThreadLocal<AffinityFunctionContext> currentAffCtx = new ThreadLocal<>();

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param baseFunc Function to wrap.
     */
    protected PlatformAffinityFunctionTarget(PlatformContext platformCtx, AffinityFunction baseFunc) {
        super(platformCtx);

        assert baseFunc != null;
        this.baseFunc = baseFunc;

        try {
            platformCtx.kernalContext().resource().injectGeneric(baseFunc);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        if (type == OP_PARTITION)
            return baseFunc.partition(reader.readObjectDetached());
        else if (type == OP_REMOVE_NODE) {
            baseFunc.removeNode(reader.readUuid());

            return 0;
        }

        return super.processInStreamOutLong(type, reader);
    }

    /** {@inheritDoc} */
    @Override public void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException {
        if (type == OP_ASSIGN_PARTITIONS) {
            AffinityFunctionContext affCtx = currentAffCtx.get();

            if (affCtx == null)
                throw new IgniteException("Thread-local AffinityFunctionContext is null. " +
                        "This may indicate an unsupported call to the base AffinityFunction.");

            final List<List<ClusterNode>> partitions = baseFunc.assignPartitions(affCtx);

            PlatformAffinityUtils.writePartitionAssignment(partitions, writer, platformContext());

            return;
        }

        super.processOutStream(type, writer);
    }

    /**
     * Sets the context for current operation.
     *
     * @param ctx Context.
     */
    void setCurrentAffinityFunctionContext(AffinityFunctionContext ctx) {
        currentAffCtx.set(ctx);
    }
}
