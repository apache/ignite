/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.security;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.os.GridOsSecurityProcessor;
import org.apache.ignite.plugin.security.SecurityCredentials;

/**
 * Test security processor.
 */
public class GridTestSecurityProcessor extends GridOsSecurityProcessor {
    /** Auth count. */
    private final AtomicInteger selfAuth;

    /** Remote auth. */
    private final Map<UUID,List<UUID>> rmAuth;

    /** Is global. */
    private final boolean global;

    /** Permissions map. */
    private Map<SecurityCredentials, TestSecurityPermissionSet> permsMap;

    /**
     * @param ctx Context.
     * @param selfAuth Local authentication counter.
     * @param rmAuth Map for count remote authentication.
     * @param global Is global authentication.
     * @param permsMap Permission map.
     */
    public GridTestSecurityProcessor(
            GridKernalContext ctx,
            AtomicInteger selfAuth,
            Map<UUID, List<UUID>> rmAuth,
            boolean global,
            Map<SecurityCredentials, TestSecurityPermissionSet> permsMap
    ) {
        super(ctx);

        this.selfAuth = selfAuth;
        this.global = global;
        this.rmAuth = rmAuth;
        this.permsMap = permsMap;
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return global;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) throws IgniteCheckedException {
        checkAuth(node);

        TestSecurityPermissionSet permsSet = permsMap.get(cred);

        return new TestSecurityContext(new TestSecuritySubject((String) cred.getLogin(), permsSet));
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
    }

    /**
     * @param node Node.
     */
    private void checkAuth(ClusterNode node){
        UUID locId = ctx.discovery().localNode().id();
        UUID rmId = node.id();

        if (rmId.equals(locId))
            selfAuth.incrementAndGet();
        else {
            List<UUID> auth = rmAuth.get(locId);

            if (auth == null) {
                ArrayList<UUID> ls = new ArrayList<>();

                ls.add(node.id());

                rmAuth.put(locId, ls);
            }
            else
                auth.add(rmId);
        }
    }
}
