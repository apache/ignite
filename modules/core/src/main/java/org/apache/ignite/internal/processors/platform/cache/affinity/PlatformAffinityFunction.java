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
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
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
public class PlatformAffinityFunction implements AffinityFunction, Externalizable {
    /** */
    private Object userFunc;

    /** */
    private transient PlatformContext ctx;

    /** */
    private transient long ptr;

    /**
     * Ctor for serialization.
     *
     */
    public PlatformAffinityFunction() {
        // No-op.
    }

    /**
     * Ctor.
     *
     * @param func User fun object.
     */
    public PlatformAffinityFunction(Object func) {
        userFunc = func;
    }

    /** {@inheritDoc} */
    public Object getUserFunc() {
        return userFunc;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        ctx.gateway().affinityFunctionReset(ptr);
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        // TODO: JNI
        // TODO: Looks like this can be called before setIgnite
        // We can calculate this and send from .NET
        return 1024;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        // TODO: JNI
        return 0;
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        // TODO: JNI
        return null;
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        // TODO: JNI
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(userFunc);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        userFunc = in.readObject();
    }

    /**
     * Injects the Ignite.
     *
     * @param ignite Ignite.
     */
    @IgniteInstanceResource
    private void setIgnite(Ignite ignite) {
        ctx = PlatformUtils.platformContext(ignite);

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx writer = ctx.writer(out);

            writer.writeObject(userFunc);

            out.synchronize();

            ptr = ctx.gateway().affinityFunctionInit(mem.pointer());
        }
    }
}