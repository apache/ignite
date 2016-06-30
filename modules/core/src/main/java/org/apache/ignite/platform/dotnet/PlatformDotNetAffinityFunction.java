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

package org.apache.ignite.platform.dotnet;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.cache.affinity.PlatformAffinityFunction;
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
import java.util.Map;
import java.util.UUID;

/**
 * AffinityFunction implementation which can be used to configure .NET affinity function in Java Spring configuration.
 */
public class PlatformDotNetAffinityFunction implements AffinityFunction, Externalizable, LifecycleAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** .NET type name. */
    private String typName;

    /** Properties. */
    private Map<String, ?> props;

    /**
     * Partition count.
     *
     * 1) Java calls partitions() method very early (before LifecycleAware.start) during CacheConfiguration validation.
     * 2) Partition count never changes.
     * Therefore, we get the value on .NET side once, and pass it along with PlatformAffinity.
     */
    private int partitions;

    /** Inner function. */
    private transient PlatformAffinityFunction func;

    /** Ignite. */
    private transient Ignite ignite;

    /**
     * Gets .NET type name.
     *
     * @return .NET type name.
     */
    public String getTypeName() {
        return typName;
    }

    /**
     * Sets .NET type name.
     *
     * @param typName .NET type name.
     */
    public void setTypeName(String typName) {
        this.typName = typName;
    }

    /**
     * Get properties.
     *
     * @return Properties.
     */
    public Map<String, ?> getProperties() {
        return props;
    }

    /**
     * Set properties.
     *
     * @param props Properties.
     */
    public void setProperties(Map<String, ?> props) {
        this.props = props;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        assert func != null;

        func.reset();
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return partitions;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        assert func != null;

        return func.partition(key);
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        assert func != null;

        return func.assignPartitions(affCtx);
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        assert func != null;

        func.removeNode(nodeId);
    }

    /**
     * Writes this func to the writer.
     *
     * @param writer Writer.
     */
    public void write(BinaryRawWriter writer) {
        assert writer != null;

        writer.writeObject(typName);
        writer.writeMap(props);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(typName);
        out.writeObject(props);
        out.writeInt(partitions);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        typName = (String)in.readObject();
        props = (Map<String, ?>)in.readObject();
        partitions = in.readInt();
    }

    /**
     * Initializes the partitions count.
     *
     * @param partitions Number of partitions.
     */
    public void initPartitions(int partitions) {
        this.partitions = partitions;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        assert ignite != null;

        PlatformContext ctx = PlatformUtils.platformContext(ignite);
        assert ctx != null;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();
            BinaryRawWriterEx writer = ctx.writer(out);

            write(writer);

            out.synchronize();

            long ptr = ctx.gateway().affinityFunctionInit(mem.pointer());

            func = new PlatformAffinityFunction(ctx, ptr, partitions);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        if (func != null)
            func.stop();
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
