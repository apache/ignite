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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;

import static org.apache.ignite.internal.processors.security.impl.TestAdditionalSecurityPluginProvider.ADDITIONAL_SECURITY_CLIENT_VERSION;
import static org.apache.ignite.internal.processors.security.impl.TestAdditionalSecurityPluginProvider.ADDITIONAL_SECURITY_CLIENT_VERSION_ATTR;

/**
 * Security processor for test.
 */
public class TestAdditionalSecurityProcessor extends TestSecurityProcessor {
    /** Client that has system permissions. */
    public static final String CLIENT = "client";

    /** Check SSL certificates flag. */
    private final boolean checkSslCerts;

    /**
     * Constructor.
     */
    public TestAdditionalSecurityProcessor(GridKernalContext ctx, TestSecurityData nodeSecData,
        Collection<TestSecurityData> predefinedAuthData, boolean globalAuth, boolean checkSslCerts) {
        super(ctx, nodeSecData, predefinedAuthData, globalAuth);

        this.checkSslCerts = checkSslCerts;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred)
        throws IgniteCheckedException {
        if (checkSslCerts && !ctx.localNodeId().equals(node.id())) {
            String str = node.attribute(ADDITIONAL_SECURITY_CLIENT_VERSION_ATTR);

            if (str == null) {
                log.info("Client version is not found.");

                return null;
            }

            if (!ADDITIONAL_SECURITY_CLIENT_VERSION.equals(str)) {
                log.info("Incorrect client version.");

                return null;
            }
        }

        return super.authenticateNode(node, cred);
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext authCtx) throws IgniteCheckedException {
        if (checkSslCerts) {
            String str = (String) authCtx.nodeAttributes().get(ADDITIONAL_SECURITY_CLIENT_VERSION_ATTR);

            if (str == null)
                throw new IgniteAccessControlException("Client version is not found.");

            if (ADDITIONAL_SECURITY_CLIENT_VERSION.equals(str)) {
                String login = (String) authCtx.credentials().getLogin();

                if (login == null || !login.contains(CLIENT)) {
                    throw new IgniteAccessControlException("User isn't allowed to use client [login=" +
                        login + ']');
                }
            }
            else
                throw new IgniteAccessControlException("Incorrect client version.");
        }

        return super.authenticate(authCtx);
    }
}
