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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTargetProxyImpl;
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
    private static final byte FLAG_PARTITION = 1;

    /** */
    private static final byte FLAG_REMOVE_NODE = 1 << 1;

    /** */
    private static final byte FLAG_ASSIGN_PARTITIONS = 1 << 2;

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

    /** */
    private transient PlatformAffinityFunctionTarget baseTarget;


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
     * Gets the user func object.
     *
     * @return User func object.
     */
    public Object getUserFunc() {
        return userFunc;
    }

    /**
     * Gets the base func.
     *
     * @return Base func.
     */
    public AffinityFunction getBaseFunc() {
        return baseFunc;
    }

    /**
     * Gets the override flags.
     *
     * @return The override flags
     */
    public byte getOverrideFlags() {
        return overrideFlags;
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
        if ((overrideFlags & FLAG_PARTITION) == 0) {
            assert baseFunc != null;

            return baseFunc.partition(key);
        }

        assert ctx != null;
        assert ptr != 0;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();
            BinaryRawWriterEx writer = ctx.writer(out);

            writer.writeLong(ptr);
            writer.writeObject(key);

            out.synchronize();

            return ctx.gateway().affinityFunctionPartition(mem.pointer());
        }
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        if ((overrideFlags & FLAG_ASSIGN_PARTITIONS) == 0) {
            assert baseFunc != null;

            return baseFunc.assignPartitions(affCtx);
        }

        assert ctx != null;
        assert ptr != 0;
        assert affCtx != null;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();
            BinaryRawWriterEx writer = ctx.writer(out);

            writer.writeLong(ptr);

            // Write previous assignment
            PlatformAffinityUtils.writeAffinityFunctionContext(affCtx, writer, ctx);

            out.synchronize();

            // Call platform
            // We can not restore original AffinityFunctionContext after the call to platform,
            // due to DiscoveryEvent (when node leaves, we can't get it by id anymore).
            // Secondly, AffinityFunctionContext can't be changed by the user.
            if (baseTarget != null)
                baseTarget.setCurrentAffinityFunctionContext(affCtx);

            try {
                ctx.gateway().affinityFunctionAssignPartitions(mem.pointer());
            }
            finally {
                if (baseTarget != null)
                    baseTarget.setCurrentAffinityFunctionContext(null);
            }

            // Read result
            return PlatformAffinityUtils.readPartitionAssignment(ctx.reader(mem), ctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        if ((overrideFlags & FLAG_REMOVE_NODE) == 0) {
            assert baseFunc != null;

            baseFunc.removeNode(nodeId);

            return;
        }

        assert ctx != null;
        assert ptr != 0;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();
            BinaryRawWriterEx writer = ctx.writer(out);

            writer.writeLong(ptr);
            writer.writeUuid(nodeId);

            out.synchronize();

            ctx.gateway().affinityFunctionRemoveNode(mem.pointer());
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
        // userFunc is null when there is nothing overridden
        if (userFunc == null)
            return;

        assert ignite != null;
        ctx = PlatformUtils.platformContext(ignite);
        assert ctx != null;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();
            BinaryRawWriterEx writer = ctx.writer(out);

            writer.writeObject(userFunc);

            out.synchronize();

            baseTarget = baseFunc != null
                ? new PlatformAffinityFunctionTarget(ctx, baseFunc)
                : null;

            PlatformTargetProxyImpl baseTargetProxy = baseTarget != null
                    ? new PlatformTargetProxyImpl(baseTarget, ctx)
                    : null;

            ptr = ctx.gateway().affinityFunctionInit(mem.pointer(), baseTargetProxy);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        if (ptr == 0)
            return;

        assert ctx != null;

        ctx.gateway().affinityFunctionDestroy(ptr);
    }

    /**
     * Injects the Ignite.
     *
     * @param ignite Ignite.
     */
    @SuppressWarnings("unused")
    @IgniteInstanceResource
    public void setIgnite(Ignite ignite) throws IgniteCheckedException {
        this.ignite = ignite;

        if (baseFunc != null && ignite != null)
            ((IgniteEx)ignite).context().resource().injectGeneric(baseFunc);
    }
}