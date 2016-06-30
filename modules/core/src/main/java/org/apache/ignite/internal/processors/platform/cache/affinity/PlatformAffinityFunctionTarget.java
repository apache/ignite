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

package org.apache.ignite.internal.processors.platform.cache.affinity;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;

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
    }

    /** {@inheritDoc} */
    @Override protected long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        if (type == OP_PARTITION)
            return baseFunc.partition(reader.readObjectDetached());
        else if (type == OP_REMOVE_NODE) {
            baseFunc.removeNode(reader.readUuid());
            return 0;
        }

        return super.processInStreamOutLong(type, reader);
    }

    /** {@inheritDoc} */
    @Override protected void processInStreamOutStream(int type, BinaryRawReaderEx reader,
        BinaryRawWriterEx writer) throws IgniteCheckedException {
        if (type == OP_ASSIGN_PARTITIONS) {
            // TODO: read context
            baseFunc.assignPartitions(null);

            // TODO: write result
            return;
        }

        super.processInStreamOutStream(type, reader, writer);
    }
}
