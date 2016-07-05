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
    private transient String typName;

    /** Properties. */
    private transient Map<String, ?> props;

    /** Inner function. */
    private PlatformAffinityFunction func;

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
        assert func != null;

        return func.partitions();
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

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(func);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        func = (PlatformAffinityFunction) in.readObject();
    }

    /**
     * Initializes this instance.
     *
     * @param func Underlying func.
     */
    public void init(PlatformAffinityFunction func) {
        assert func != null;

        this.func = func;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        assert func != null;

        func.start();
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        assert func != null;

        func.stop();
    }

    /**
     * Gets the inner func.
     *
     * @return The inner func.
     */
    public PlatformAffinityFunction getFunc() {
        return func;
    }

    /**
     * Injects the Ignite.
     *
     * @param ignite Ignite.
     */
    @SuppressWarnings("unused")
    @IgniteInstanceResource
    private void setIgnite(Ignite ignite) {
        assert func != null;

        func.setIgnite(ignite);
    }
}
