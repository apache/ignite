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

package org.apache.ignite.internal.processor.security;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;

import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_NODE;

/**
 * Security processor for test.
 */
public class TestSecurityProcessor extends GridProcessorAdapter implements GridSecurityProcessor {
    /** Permissions. */
    private static final Map<SecurityCredentials, SecurityPermissionSet> PERMS = new ConcurrentHashMap<>();

    /** Config. */
    private TestSecurityPluginConfiguration cfg;

    /**
     * @param ctx Context.
     */
    public TestSecurityProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {

        return new TestSecurityContext(
            new TestSecuritySubject()
                .setType(REMOTE_NODE)
                .setId(node.id())
                .setAddr(new InetSocketAddress(F.first(node.addresses()), 0))
                .setLogin(cred.getUserObject())
                .setPerms(PERMS.get(cred))
        );
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm, SecurityContext securityCtx)
        throws SecurityException {

        assert securityCtx instanceof TestSecurityContext;

        if (!((TestSecurityContext)securityCtx).operationAllowed(name, perm))
            throw new SecurityException("Authorization failed [perm=" + perm +
                ", name=" + name +
                ", subject=" + securityCtx.subject() + ']');
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        SecurityCredentials cred = new SecurityCredentials(
            configuration().getLogin(), configuration().getPwd(), configuration().getUserObj()
        );

        PERMS.put(cred, configuration().getPermissions());

        ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS, cred);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        PERMS.remove(ctx.nodeAttribute(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS));
    }

    /**
     * Security configuration.
     */
    private TestSecurityPluginConfiguration configuration() {
        if (cfg == null) {
            IgniteConfiguration igniteCfg = ctx.config();

            if (igniteCfg.getPluginConfigurations() != null) {
                for (PluginConfiguration pluginCfg : igniteCfg.getPluginConfigurations()) {
                    if (pluginCfg instanceof TestSecurityPluginConfiguration) {
                        cfg = (TestSecurityPluginConfiguration)pluginCfg;

                        break;
                    }
                }
            }
        }

        assert cfg != null;

        return cfg;
    }
}
