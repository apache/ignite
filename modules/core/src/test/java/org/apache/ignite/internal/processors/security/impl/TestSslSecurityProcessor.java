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

package org.apache.ignite.internal.processors.security.impl;

import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_CERTIFICATES;

/**
 * Security processor for test.
 */
public class TestSslSecurityProcessor extends TestSecurityProcessor {
    private final boolean checkSslCerts;

    /**
     * Constructor.
     */
    public TestSslSecurityProcessor(GridKernalContext ctx, TestSecurityData nodeSecData,
        Collection<TestSecurityData> predefinedAuthData, boolean checkSslCerts) {
        super(ctx, nodeSecData, predefinedAuthData);

        this.checkSslCerts = checkSslCerts;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {
        if (checkSslCerts && node.attributes().get(ATTR_SECURITY_CERTIFICATES) == null &&
            !ctx.localNodeId().equals(node.id())) {
            log.info("SSL certificates are not found.");

            return null;
        }

        return super.authenticateNode(node, cred);
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) {
        if (checkSslCerts && ctx.nodeAttributes().get(ATTR_SECURITY_CERTIFICATES) == null) {
            log.info("SSL certificates are not found.");

            return null;
        }

        return super.authenticate(ctx);
    }
}
