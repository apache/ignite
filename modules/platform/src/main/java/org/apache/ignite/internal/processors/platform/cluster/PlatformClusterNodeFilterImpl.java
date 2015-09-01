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

package org.apache.ignite.internal.processors.platform.cluster;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.portable.PortableRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractPredicate;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Interop cluster node filter.
 */
public class PlatformClusterNodeFilterImpl extends PlatformAbstractPredicate implements PlatformClusterNodeFilter {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * {@link java.io.Externalizable} support.
     */
    public PlatformClusterNodeFilterImpl() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param pred .Net portable predicate.
     * @param ctx Kernal context.
     */
    public PlatformClusterNodeFilterImpl(Object pred, PlatformContext ctx) {
        super(pred, 0, ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean apply(ClusterNode clusterNode) {
        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx writer = ctx.writer(out);

            writer.writeObject(pred);
            ctx.writeNode(writer, clusterNode);

            out.synchronize();

            return ctx.gateway().clusterNodeFilterApply(mem.pointer()) != 0;
        }
    }

    /**
     * @param ignite Ignite instance.
     */
    @SuppressWarnings("UnusedDeclaration")
    @IgniteInstanceResource
    public void setIgniteInstance(Ignite ignite) {
        ctx = PlatformUtils.platformContext(ignite);
    }
}