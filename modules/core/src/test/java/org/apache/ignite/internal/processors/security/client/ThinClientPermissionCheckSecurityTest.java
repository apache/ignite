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

package org.apache.ignite.internal.processors.security.client;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSslSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_CERTIFICATES;
import static org.apache.ignite.internal.processors.security.impl.TestSslSecurityProcessor.CLIENT;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Security tests for thin client.
 */
@RunWith(JUnit4.class)
public class ThinClientPermissionCheckSecurityTest extends ThinClientPermissionCheckTest {
    /** {@inheritDoc} */
    @Override protected AbstractTestSecurityPluginProvider securityPluginProvider(String instanceName,
        TestSecurityData... clientData) {
        return new TestSslSecurityPluginProvider("srv_" + instanceName, null, ALLOW_ALL, false, true, clientData);
    }

    /** {@inheritDoc} */
    @Override protected Map<String, Object> userAttributres() {
        String clientJksPath = U.getIgniteHome() + "/modules/clients/src/test/keystore/client.jks";

        try (InputStream ksInputStream = new FileInputStream(clientJksPath)) {
            KeyStore clientKeyStore = KeyStore.getInstance("JKS");
            clientKeyStore.load(ksInputStream, "123456".toCharArray());

            Certificate[] certs = clientKeyStore.getCertificateChain(CLIENT);
            JdkMarshaller marshaller = MarshallerUtils.jdkMarshaller(null);

            return Collections.singletonMap(ATTR_SECURITY_CERTIFICATES, U.marshal(marshaller, certs));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
