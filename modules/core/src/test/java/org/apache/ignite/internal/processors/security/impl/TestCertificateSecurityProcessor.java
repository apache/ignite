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

import java.net.InetSocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Security processor for test.
 */
public class TestCertificateSecurityProcessor extends GridProcessorAdapter implements GridSecurityProcessor {
    /** Permissions. */
    public static final Map<String, SecurityPermissionSet> PERMS = new ConcurrentHashMap<>();

    /** Users security data. */
    private final Collection<TestSecurityData> predefinedAuthData;

    /**
     * Constructor.
     */
    public TestCertificateSecurityProcessor(GridKernalContext ctx, Collection<TestSecurityData> predefinedAuthData) {
        super(ctx);

        this.predefinedAuthData = predefinedAuthData.isEmpty()
            ? Collections.emptyList()
            : new ArrayList<>(predefinedAuthData);
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {
        return new TestSecurityContext(
            new TestSecuritySubject()
                .setType(REMOTE_NODE)
                .setId(node.id())
                .setAddr(new InetSocketAddress(F.first(node.addresses()), 0))
                .setLogin("")
                .setPerms(ALLOW_ALL)
        );
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) {
        Certificate[] certs = ctx.certificates();

        assertNotNull(certs);

        assertEquals(2, certs.length);

        assertTrue(((X509Certificate)certs[0]).getSubjectDN().getName().matches("^CN=[a-z0-9]+$"));
        assertTrue(((X509Certificate)certs[0]).getIssuerDN().getName().startsWith("C=RU, ST=SPb, L=SPb, O=Ignite, OU=Dev"));

        String cn = ((X509Certificate)certs[0]).getSubjectDN().getName().substring(3);

        if (!PERMS.containsKey(cn))
            return null;

        return new TestSecurityContext(
            new TestSecuritySubject()
                .setType(ctx.subjectType())
                .setId(ctx.subjectId())
                .setAddr(ctx.address())
                .setLogin(cn)
                .setPerms(PERMS.get(cn))
                .setCerts(ctx.certificates())
        );
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm, SecurityContext securityCtx)
        throws SecurityException {

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

        ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS, new SecurityCredentials("", ""));

        for (TestSecurityData data : predefinedAuthData)
            PERMS.put(data.credentials().getLogin().toString(), data.getPermissions());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        for (TestSecurityData data : predefinedAuthData)
            PERMS.remove(data.credentials().getLogin().toString());
    }
}
