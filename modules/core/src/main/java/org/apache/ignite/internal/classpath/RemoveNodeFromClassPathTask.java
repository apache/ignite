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

public class RemoveNodeFromClassPathTask extends ClassPathProcessor.ClassPathTask<Void> {
    /** Node to remove from {@link IgniteClassPath#deployedOnNodes()} set. */
    private final UUID node2rmv;

    /** */
    public RemoveNodeFromClassPathTask(GridKernalContext ctx, UUID icpId, UUID node2rmv) {
        super(ctx, icpId);
        this.node2rmv = node2rmv;
    }

    /** {@inheritDoc} */
    @Override void start() {
        ctx.classPath()
            .modifyInMetastorageAsync(icpId, null, icp -> icp.removeDeployeOnNode(node2rmv))
            .listen(this::finishTaskWithFutureResult);
    }

    /** {@inheritDoc} */
    @Override String name() {
        return "Remove node";
    }

    /** {@inheritDoc} */
    @Override void ok() {
        if (log.isDebugEnabled())
            log.debug("Node removed from ClassPath [icpId=" + icpId + ", node2rmv=" + node2rmv + ']');
    }

    /** {@inheritDoc} */
    @Override void fail(Throwable t) {
        log.warning("Fail to remove node from ClassPath [icpId=" + icpId + ", node2rmv=" + node2rmv + ']', t);
    }
}
