package org.apache.ignite.ssl;

import org.apache.ignite.testframework.GridTestUtils;

import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;

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
