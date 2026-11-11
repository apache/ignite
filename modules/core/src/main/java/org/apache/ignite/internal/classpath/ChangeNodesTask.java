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
package org.apache.ignite.internal.classpath;

import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;

/**
 * Removes or adds {@link #node} to {@link IgniteClassPath#deployedOnNodes()} set.
 */
public class ChangeNodesTask extends ClassPathProcessor.ClassPathTask<Void> {
    /** Node. */
    private final UUID node;

    /** Add flag. */
    private final boolean add;

    /** */
    private ChangeNodesTask(GridKernalContext ctx, UUID icpId, UUID node, boolean add) {
        super(ctx, icpId);
        this.node = node;
        this.add = add;
    }

    /** */
    public static ChangeNodesTask removeNode(GridKernalContext ctx, UUID icpId, UUID node) {
        return new ChangeNodesTask(ctx, icpId, node, false);
    }

    /** */
    public static ChangeNodesTask addNode(GridKernalContext ctx, UUID icpId, UUID node) {
        return new ChangeNodesTask(ctx, icpId, node, true);
    }

    /** {@inheritDoc} */
    @Override void start0() {
        ctx.classPath()
            .modifyInMetastorageAsync(icpId, null, icp -> add ? icp.addDeployedOnNode(node) : icp.removeDeployedOnNode(node), this::stopped)
            .listen(this::finishTaskWithFutureResult);
    }

    /** {@inheritDoc} */
    @Override String name() {
        return add ? "add node" : "remove node";
    }

    /** {@inheritDoc} */
    @Override void ok() {
        if (log.isDebugEnabled())
            log.debug("ClassPath nodes changed [icpId=" + icpId + ", node=" + node + ", add=" + add + ']');
    }

    /** {@inheritDoc} */
    @Override void fail(Throwable t) {
        log.warning("Fail to change ClassPath nodes [icpId=" + icpId + ", node=" + node + ", add=" + add + ']', t);
    }
}
