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
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.cache.configuration.Factory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.util.typedef.F;

/**
 * SSL utility method to create SSL connetion.
 */
public class JdbcThinSSLUtil {
    /** Trust all certificates manager. */
    private final static X509TrustManager TRUST_ALL_MANAGER = new X509TrustManager() {
        @Override public X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        @Override public void checkServerTrusted(X509Certificate[] arg0, String arg1) {
        }

        @Override public void checkClientTrusted(X509Certificate[] arg0, String arg1) {
        }
    };

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
    public static SSLSocket createSSLSocket(InetSocketAddress addr, ConnectionProperties connProps) throws SQLException, IOException {
        try {
            SSLSocketFactory sslSocketFactory = getSSLSocketFactory(connProps);

            SSLSocket sock = (SSLSocket)sslSocketFactory.createSocket(addr.getAddress(), addr.getPort());

            sock.setUseClientMode(true);

            sock.startHandshake();

            return sock;
        }
        catch (IOException e) {
            throw new SQLException("Failed to SSL connect to server [url=" + connProps.getUrl() +']',
                SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
    }

   /**
     * @param connProps Connection properties.
     * @return SSL socket factory.
     * @throws SQLException On error.
     */
    private static SSLSocketFactory getSSLSocketFactory(ConnectionProperties connProps) throws SQLException {
        String sslFactory = connProps.getSslFactory();
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
            && sslProtocol == null) {
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

        if (!F.isEmpty(cliCertKeyStoreUrl))
            cliCertKeyStoreUrl = checkAndConvertUrl(cliCertKeyStoreUrl);

        if (!F.isEmpty(trustCertKeyStoreUrl))
            trustCertKeyStoreUrl = checkAndConvertUrl(trustCertKeyStoreUrl);

        TrustManagerFactory tmf;
        KeyManagerFactory kmf;

        KeyManager[] kms = null;

        try {
            tmf = TrustManagerFactory.getInstance(keyAlgorithm);

            kmf = KeyManagerFactory.getInstance(keyAlgorithm);
        }
        catch (NoSuchAlgorithmException e) {
            throw new SQLException("Default algorithm definitions for TrustManager and/or KeyManager are invalid." +
                " Check java security properties file.", SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }

        InputStream ksInputStream = null;

        try {
            if (!F.isEmpty(cliCertKeyStoreUrl) && !F.isEmpty(cliCertKeyStoreType)) {
                KeyStore clientKeyStore = KeyStore.getInstance(cliCertKeyStoreType);

                URL ksURL = new URL(cliCertKeyStoreUrl);

                char[] password = (cliCertKeyStorePwd == null) ? new char[0] : cliCertKeyStorePwd.toCharArray();

                ksInputStream = ksURL.openStream();

                clientKeyStore.load(ksInputStream, password);

                kmf.init(clientKeyStore, password);

                kms = kmf.getKeyManagers();
            }
        }
        catch (UnrecoverableKeyException e) {
            throw new SQLException("Could not recover keys from client keystore.",
                SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        catch (NoSuchAlgorithmException e) {
            throw new SQLException("Unsupported keystore algorithm.", SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        catch (KeyStoreException e) {
            throw new SQLException("Could not create client KeyStore instance.",
                SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        catch (CertificateException e) {
            throw new SQLException("Could not load client key store. [storeType=" + cliCertKeyStoreType
                + ", cliStoreUrl=" + cliCertKeyStoreUrl + ']', SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        catch (MalformedURLException e) {
            throw new SQLException("Invalid client key store URL. [url=" + cliCertKeyStoreUrl + ']',
                SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        catch (IOException e) {
            throw new SQLException("Could not open client key store.[url=" + cliCertKeyStoreUrl + ']',
                SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        finally {
            if (ksInputStream != null) {
                try {
                    ksInputStream.close();
                }
                catch (IOException e) {
                    // can't close input stream, but keystore can be properly initialized
                    // so we shouldn't throw this exception
                }
            }
        }

        InputStream tsInputStream = null;

        List<TrustManager> tms;

        if (connProps.isSslTrustAll())
            tms = Collections.<TrustManager>singletonList(TRUST_ALL_MANAGER);
        else {
            tms = new ArrayList<>();

            try {
                KeyStore trustKeyStore = null;

                if (!F.isEmpty(trustCertKeyStoreUrl) && !F.isEmpty(trustCertKeyStoreType)) {
                    char[] trustStorePassword = (trustCertKeyStorePwd == null) ? new char[0]
                        : trustCertKeyStorePwd.toCharArray();

                    tsInputStream = new URL(trustCertKeyStoreUrl).openStream();

                    trustKeyStore = KeyStore.getInstance(trustCertKeyStoreType);

                    trustKeyStore.load(tsInputStream, trustStorePassword);
                }

                tmf.init(trustKeyStore);

                TrustManager[] origTms = tmf.getTrustManagers();

                Collections.addAll(tms, origTms);
            }
            catch (NoSuchAlgorithmException e) {
                throw new SQLException("Unsupported keystore algorithm.", SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
            catch (KeyStoreException e) {
                throw new SQLException("Could not create trust KeyStore instance.",
                    SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
            catch (CertificateException e) {
                throw new SQLException("Could not load trusted key store. [storeType=" + trustCertKeyStoreType +
                    ", cliStoreUrl=" + trustCertKeyStoreUrl + ']', SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
            catch (MalformedURLException e) {
                throw new SQLException("Invalid trusted key store URL. [url=" + trustCertKeyStoreUrl + ']',
                    SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
            catch (IOException e) {
                throw new SQLException("Could not open trusted key store. [url=" + cliCertKeyStoreUrl + ']',
                    SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
            finally {
                if (tsInputStream != null) {
                    try {
                        tsInputStream.close();
                    }
                    catch (IOException e) {
                        // can't close input stream, but keystore can be properly initialized
                        // so we shouldn't throw this exception
                    }
                }
            }
        }

        assert !tms.isEmpty();

        try {
            SSLContext sslContext = SSLContext.getInstance(sslProtocol);

            sslContext.init(kms, tms.toArray(new TrustManager[tms.size()]), null);

            return sslContext.getSocketFactory();
        }
        catch (NoSuchAlgorithmException e) {
            throw new SQLException(sslProtocol + " is not a valid SSL protocol.", SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        catch (KeyManagementException e) {
            throw new SQLException("Cannot init SSL context.", SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
    }

    /**
     * @param url URL or path to check and convert to URL.
     * @return URL.
     * @throws SQLException If URL is invalid.
     */
    private static String checkAndConvertUrl(String url) throws SQLException {
        try {
            return new URL(url).toString();
        }
        catch (MalformedURLException e) {
            try {
                return FileSystems.getDefault().getPath(url).toUri().toURL().toString();
            }
            catch (MalformedURLException e1) {
                throw new SQLException("Invalid keystore UR: " + url,
                    SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
        }
    }
}