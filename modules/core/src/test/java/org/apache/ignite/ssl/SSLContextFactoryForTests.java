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

package org.apache.ignite.ssl;

import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.testframework.GridTestUtils;

public class SSLContextFactoryForTests {

    /**
     * @return SSL context factory to use on server nodes for communication between nodes in a cluster.
     */
    public static Factory<SSLContext> serverSSLFactory() {
        return GridTestUtils.sslTrustedFactory("server", "trustone");
    }

    /**
     * @return SSL context factory to use on client nodes for communication between nodes in a cluster.
     */
    public static Factory<SSLContext> clientSSLFactory() {
        return GridTestUtils.sslTrustedFactory("client", "trustone");
    }

    /**
     * @return SSL context factory to use in client connectors.
     */
    public static Factory<SSLContext> clientConnectorSSLFactory() {
        return GridTestUtils.sslTrustedFactory("thinServer", "trusttwo");
    }

    /**
     * @return SSL context factory to use in thin clients.
     */
    public static Factory<SSLContext> thinClientSSLFactory() {
        return GridTestUtils.sslTrustedFactory("thinClient", "trusttwo");
    }

    /**
     * @return SSL context factory to use in thin clients for check DisabledTrustManager.
     */
    public static Factory<SSLContext> thinClientDisabledTrustManagerSSLFactory() {

        SslContextFactory factory = (SslContextFactory) GridTestUtils.sslTrustedFactory("thinClient", "trustthree");
        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /**
     * @return Wrong SSL context factory to use in thin clients for check wrong certificate.
     */
    public static Factory<SSLContext> thinClientSSLFactoryWithWrongTrustCertificate() {
        return GridTestUtils.sslTrustedFactory("thinClient", "trustone");
    }

    /**
     * @return SSL context factory to use in client connectors.
     */
    public static Factory<SSLContext> connectorSSLFactory() {
        return GridTestUtils.sslTrustedFactory("connectorServer", "trustthree");
    }

}
