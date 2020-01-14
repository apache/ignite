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

package org.apache.ignite.internal.jdbc.thin;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.ssl.SslContextFactory;

/**
 * SSL utility method to create SSL connetion.
 */
public class JdbcThinSSLUtil {
    /** Trust all certificates manager. */
    private static final X509TrustManager TRUST_ALL_MANAGER = new X509TrustManager() {
        @Override public X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        @Override public void checkServerTrusted(X509Certificate[] arg0, String arg1) {
        }

        @Override public void checkClientTrusted(X509Certificate[] arg0, String arg1) {
        }
    };

    /** Empty char array. */
    public static final char[] EMPTY_CHARS = new char[0];

    /**
     *
     */
    private JdbcThinSSLUtil() {
        // No-op.
    }

    /**
     * @param addr Connection address.
     * @param connProps Connection properties.
     * @throws SQLException On connection error or reject.
     * @throws IOException On IO error in handshake.
     * @return SSL socket.
     */
    public static SSLSocket createSSLSocket(InetSocketAddress addr, ConnectionProperties connProps) throws SQLException {
        try {
            SSLSocketFactory sslSocketFactory = getSSLSocketFactory(connProps);

            SSLSocket sock = (SSLSocket)sslSocketFactory.createSocket(addr.getAddress(), addr.getPort());

            sock.setUseClientMode(true);

            sock.startHandshake();

            return sock;
        }
        catch (IOException e) {
            throw new SQLException("Failed to SSL connect to server [url=" + connProps.getUrl() +
                " address=" + addr + ']', SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
    }

   /**
     * @param connProps Connection properties.
     * @return SSL socket factory.
     * @throws SQLException On error.
     */
    private static SSLSocketFactory getSSLSocketFactory(ConnectionProperties connProps) throws SQLException {
        String sslFactory = connProps.getSslFactory();
        String cipherSuites = connProps.getSslCipherSuites();
        String cliCertKeyStoreUrl = connProps.getSslClientCertificateKeyStoreUrl();
        String cliCertKeyStorePwd = connProps.getSslClientCertificateKeyStorePassword();
        String cliCertKeyStoreType = connProps.getSslClientCertificateKeyStoreType();
        String trustCertKeyStoreUrl = connProps.getSslTrustCertificateKeyStoreUrl();
        String trustCertKeyStorePwd = connProps.getSslTrustCertificateKeyStorePassword();
        String trustCertKeyStoreType = connProps.getSslTrustCertificateKeyStoreType();
        String sslProtocol = connProps.getSslProtocol();
        String keyAlgorithm = connProps.getSslKeyAlgorithm();

        if (!F.isEmpty(sslFactory)) {
            try {
                Class<Factory<SSLSocketFactory>> cls = (Class<Factory<SSLSocketFactory>>)JdbcThinSSLUtil.class
                    .getClassLoader().loadClass(sslFactory);

                Factory<SSLSocketFactory> f = cls.newInstance();

                return f.create();
            }
            catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                throw new SQLException("Could not fount SSL factory class: " + sslFactory,
                    SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
        }

        if (cliCertKeyStoreUrl == null && cliCertKeyStorePwd == null && cliCertKeyStoreType == null
            && trustCertKeyStoreUrl == null && trustCertKeyStorePwd == null && trustCertKeyStoreType == null
            && sslProtocol == null && cipherSuites == null) {
            try {
                return SSLContext.getDefault().getSocketFactory();
            }
            catch (NoSuchAlgorithmException e) {
                throw new SQLException("Could not create default SSL context",
                    SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
        }

        if (cliCertKeyStoreUrl == null)
            cliCertKeyStoreUrl = System.getProperty("javax.net.ssl.keyStore");

        if (cliCertKeyStorePwd == null)
            cliCertKeyStorePwd = System.getProperty("javax.net.ssl.keyStorePassword");

        if (cliCertKeyStoreType == null)
            cliCertKeyStoreType = System.getProperty("javax.net.ssl.keyStoreType", "JKS");

        if (trustCertKeyStoreUrl == null)
            trustCertKeyStoreUrl = System.getProperty("javax.net.ssl.trustStore");

        if (trustCertKeyStorePwd == null)
            trustCertKeyStorePwd = System.getProperty("javax.net.ssl.trustStorePassword");

        if (trustCertKeyStoreType == null)
            trustCertKeyStoreType = System.getProperty("javax.net.ssl.trustStoreType", "JKS");

        if (sslProtocol == null)
            sslProtocol = "TLS";

        SslContextFactory f = new SslContextFactory();

        f.setProtocol(sslProtocol);

        f.setKeyAlgorithm(keyAlgorithm);

        f.setKeyStoreFilePath(cliCertKeyStoreUrl);
        f.setKeyStoreType(cliCertKeyStoreType);
        f.setKeyStorePassword((cliCertKeyStorePwd == null) ? EMPTY_CHARS :
            cliCertKeyStorePwd.toCharArray());

        if (connProps.isSslTrustAll())
            f.setTrustManagers(TRUST_ALL_MANAGER);
        else {
            f.setTrustStoreFilePath(trustCertKeyStoreUrl);
            f.setTrustStoreType(trustCertKeyStoreType);
            f.setTrustStorePassword((trustCertKeyStorePwd == null) ? EMPTY_CHARS
                : trustCertKeyStorePwd.toCharArray());
        }

        if (!F.isEmpty(cipherSuites))
            f.setCipherSuites(cipherSuites.split(","));

        try {
            final SSLContext sslContext = f.create();

            return sslContext.getSocketFactory();
        }
        catch (IgniteException e) {
            final Throwable cause = e.getCause();

            // Unwrap.
            if (cause instanceof SSLException)
                throw new SQLException(cause.getMessage(), SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            else
                throw new SQLException("Unknown error.", SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
    }
}
