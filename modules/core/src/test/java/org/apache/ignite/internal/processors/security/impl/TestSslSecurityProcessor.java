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

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_CERTIFICATES;

/**
 * Security processor for test.
 */
public class TestSslSecurityProcessor extends TestSecurityProcessor {
    /** Client that has system permissions. */
    public static final String CLIENT_ADMIN_OPER = "client_admin_oper";

    /** Check SSL certificates flag. */
    private final boolean checkSslCerts;

    /**
     * Constructor.
     */
    public TestSslSecurityProcessor(GridKernalContext ctx, TestSecurityData nodeSecData,
        Collection<TestSecurityData> predefinedAuthData, boolean globalAuth, boolean checkSslCerts) {
        super(ctx, nodeSecData, predefinedAuthData, globalAuth);

        this.checkSslCerts = checkSslCerts;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred)
        throws IgniteCheckedException {
        if (checkSslCerts && !ctx.localNodeId().equals(node.id())) {
            Marshaller marshaller = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());
            ClassLoader ldr = U.resolveClassLoader(ctx.config());
            byte[] bytes = node.attribute(ATTR_SECURITY_CERTIFICATES);

            if (bytes == null) {
                log.info("SSL certificates are not found.");

                return null;
            }

            try {
                U.unmarshal(marshaller, bytes, ldr);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteAccessControlException("Failed to get security certificates.", e);
            }
        }

        return super.authenticateNode(node, cred);
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext authCtx) throws IgniteCheckedException {
        if (checkSslCerts) {
            Marshaller marshaller = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());
            ClassLoader ldr = U.resolveClassLoader(ctx.config());
            byte[] bytes = (byte[]) authCtx.nodeAttributes().get(ATTR_SECURITY_CERTIFICATES);

            if (bytes == null)
                throw new IgniteAccessControlException("SSL certificates are not found.");

            Certificate[] certs;

            try {
                certs = U.unmarshal(marshaller, bytes, ldr);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteAccessControlException("Failed to get security certificates.", e);
            }

            validateCertificates(certs, authCtx.credentials().getLogin());
        }

        return super.authenticate(authCtx);
    }

    /**
     * @param certs Certificates.
     * @param login Login.
     * @throws IgniteAccessControlException If received invalid certificates.
     */
    private void validateCertificates(Certificate[] certs, Object login) throws IgniteAccessControlException {
        for (Certificate cert : certs) {
            if (!(cert instanceof X509Certificate))
                continue;

            X509Certificate x509Cert = (X509Certificate) cert;

            if (x509Cert.getSubjectDN().getName().contains("CN=client")) {
                if (CLIENT_ADMIN_OPER.equals(login))
                    return;

                throw new IgniteAccessControlException("Client certificate doesn't correspond with login [login=" +
                    login + ", subjDN=" + x509Cert.getSubjectDN().getName() + "]");
            }
        }

        throw new IgniteAccessControlException("Client certificate not found.");
    }
}
