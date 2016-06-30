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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.UUID;

/**
 * Platform AffinityFunction.
 */
public class PlatformAffinityFunction implements AffinityFunction, Externalizable, LifecycleAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Object userFunc;

    /**
     * Partition count.
     *
     * 1) Java calls partitions() method very early (before LifecycleAware.start) during CacheConfiguration validation.
     * 2) Partition count never changes.
     * Therefore, we get the value on .NET side once, and pass it along with PlatformAffinity.
     */
    private int partitions;

    /** */
    private AffinityFunction baseFunc;

    /** */
    private byte overrideFlags;

    /** */
    private transient Ignite ignite;

    /** */
    private transient PlatformContext ctx;

    /** */
    private transient long ptr;


    /**
     * Ctor for serialization.
     *
     */
    public PlatformAffinityFunction() {
        partitions = -1;
    }

    /**
     * Ctor.
     *
     * @param func User fun object.
     * @param partitions Number of partitions.
     */
    public PlatformAffinityFunction(Object func, int partitions, byte overrideFlags, AffinityFunction baseFunc) {
        userFunc = func;
        this.partitions = partitions;
        this.overrideFlags = overrideFlags;
        this.baseFunc = baseFunc;
    }

    /**
     * Ctor.
     *
     * @param ptr User func ptr.
     * @param partitions Number of partitions.
     */
    public PlatformAffinityFunction(PlatformContext ctx, long ptr, int partitions) {
        this.ctx = ctx;
        this.ptr = ptr;
        this.partitions = partitions;

        ignite = ctx.kernalContext().grid();
    }

    /** {@inheritDoc} */
    public Object getUserFunc() {
        return userFunc;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // userFunc is always in initial state (it is serialized only once on start).
        if (baseFunc != null)
            baseFunc.reset();
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        // Affinity function can not return different number of partitions,
        // so we pass this value once from the platform.
        assert partitions > 0;

        return partitions;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        assert ctx != null;
        assert ptr != 0;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();
            BinaryRawWriterEx writer = ctx.writer(out);

            writer.writeObject(key);

            out.synchronize();

            return ctx.gateway().affinityFunctionPartition(ptr, mem.pointer());
        }
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        assert ctx != null;
        assert ptr != 0;
        assert affCtx != null;

        try (PlatformMemory outMem = ctx.memory().allocate()) {
            try (PlatformMemory inMem = ctx.memory().allocate()) {
                PlatformOutputStream out = outMem.output();
                BinaryRawWriterEx writer = ctx.writer(outMem);

                // Write previous assignment
                PlatformAffinityFunctionSerializer.writeAffinityFunctionContext(affCtx, writer, ctx);

                // Call platform
                out.synchronize();
                ctx.gateway().affinityFunctionAssignPartitions(ptr, outMem.pointer(), inMem.pointer());

                // Read result
                return PlatformAffinityFunctionSerializer.readPartitionAssignment(ctx.reader(inMem), ctx);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        assert ctx != null;
        assert ptr != 0;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();
            BinaryRawWriterEx writer = ctx.writer(out);

            writer.writeUuid(nodeId);

            out.synchronize();

            ctx.gateway().affinityFunctionRemoveNode(ptr, mem.pointer());
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(userFunc);
        out.writeInt(partitions);
        out.writeByte(overrideFlags);
        out.writeObject(baseFunc);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        userFunc = in.readObject();
        partitions = in.readInt();
        overrideFlags = in.readByte();
        baseFunc = (AffinityFunction)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        assert ignite != null;
        ctx = PlatformUtils.platformContext(ignite);
        assert ctx != null;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();
            BinaryRawWriterEx writer = ctx.writer(out);

            writer.writeObject(userFunc);

            out.synchronize();

            final PlatformAffinityFunctionTarget baseTarget = baseFunc != null
                ? new PlatformAffinityFunctionTarget(ctx, baseFunc)
                : null;

            ptr = ctx.gateway().affinityFunctionInit(mem.pointer(), baseTarget);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        assert ctx != null;

        ctx.gateway().affinityFunctionDestroy(ptr);
    }

    /**
     * Injects the Ignite.
     *
     * @param ignite Ignite.
     */
    @IgniteInstanceResource
    private void setIgnite(Ignite ignite) {
        this.ignite = ignite;
    }
}