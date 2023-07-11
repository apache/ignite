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

import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * SSL context factory that provides SSL context configuration with specified key and trust stores.
 *
 * This factory caches the result of the first successful attempt to create an {@link SSLContext} and always returns it
 * as a result of further invocations of the {@link SslContextFactory#create()}} method.
 * <p>
 * In some cases it is useful to disable certificate validation of client side (e.g. when connecting
 * to a server with self-signed certificate). This can be achieved by setting a disabled trust manager
 * to this factory, which can be obtained by {@link #getDisabledTrustManager()} method:
 * <pre>
 *     SslContextFactory factory = new SslContextFactory();
 *     factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());
 *     // Rest of initialization.
 * </pre>
 */
public class SslContextFactory extends AbstractSslContextFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default key / trust store type. */
    public static final String DFLT_STORE_TYPE = AbstractSslContextFactory.DFLT_STORE_TYPE;

    /** Default SSL protocol. */
    public static final String DFLT_SSL_PROTOCOL = AbstractSslContextFactory.DFLT_SSL_PROTOCOL;

    /** Default key manager / trust manager algorithm. Specifying different trust manager algorithm is not supported. */
    public static final String DFLT_KEY_ALGORITHM = AbstractSslContextFactory.DFLT_KEY_ALGORITHM;

    /** Key store type. */
    protected String keyStoreType = DFLT_STORE_TYPE;

    /** Path to key store file */
    protected String keyStoreFilePath;

    /** Key store password */
    protected char[] keyStorePwd;

    /**
     * Gets key store type used for context creation.
     *
     * @return Key store type.
     */
    public String getKeyStoreType() {
        return keyStoreType;
    }

    /**
     * Sets key store type used in context initialization. If not provided, {@link #DFLT_STORE_TYPE} will
     * be used.
     *
     * @param keyStoreType Key store type.
     */
    public void setKeyStoreType(String keyStoreType) {
        A.notNull(keyStoreType, "keyStoreType");

        this.keyStoreType = keyStoreType;
    }

    /**
     * Gets path to the key store file.
     *
     * @return Path to key store file.
     */
    public String getKeyStoreFilePath() {
        return keyStoreFilePath;
    }

    /**
     * Sets path to the key store file. This is a mandatory parameter since
     * ssl context could not be initialized without key manager.
     *
     * @param keyStoreFilePath Path to key store file.
     */
    public void setKeyStoreFilePath(String keyStoreFilePath) {
        A.notNull(keyStoreFilePath, "keyStoreFilePath");

        this.keyStoreFilePath = keyStoreFilePath;
    }

    /**
     * Gets key store password.
     *
     * @return Key store password.
     */
    public char[] getKeyStorePassword() {
        return keyStorePwd;
    }

    /**
     * Sets key store password.
     *
     * @param keyStorePwd Key store password.
     */
    public void setKeyStorePassword(char[] keyStorePwd) {
        A.notNull(keyStorePwd, "keyStorePwd");

        this.keyStorePwd = keyStorePwd;
    }

    /**
     * Returns an instance of trust manager that will always succeed regardless of certificate provided.
     *
     * @return Trust manager instance.
     */
    public static TrustManager getDisabledTrustManager() {
        return new DisabledX509TrustManager();
    }

    /** {@inheritDoc} */
    @Override protected final KeyManager[] createKeyManagers() throws SSLException {
        try {
            KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(keyAlgorithm);

            KeyStore keyStore = loadKeyStore(keyStoreType, keyStoreFilePath, keyStorePwd);

            keyMgrFactory.init(keyStore, keyStorePwd);

            return keyMgrFactory.getKeyManagers();
        }
        catch (NoSuchAlgorithmException e) {
            throw new SSLException("Unsupported key algorithm: " + keyAlgorithm, e);
        }
        catch (GeneralSecurityException e) {
            throw new SSLException("Failed to initialize Key Manager (security exception occurred) [type=" +
                keyStoreType + ", keyStorePath=" + keyStoreFilePath + ']', e);
        }
    }

    /**
     * Builds human-readable string with factory parameters.
     *
     * @return Parameters string.
     */
    private String parameters() {
        StringBuilder buf = new StringBuilder("[keyStoreType=").append(keyStoreType);

        buf.append(", proto=").append(proto).append(", keyStoreFile=").append(keyStoreFilePath);

        if (trustMgrs != null)
            buf.append(", trustMgrs=").append(Arrays.toString(trustMgrs));
        else
            buf.append(", trustStoreFile=").append(trustStoreFilePath);

        buf.append(']');

        return buf.toString();
    }

    /**
     * Checks that all required parameters are set.
     *
     * @throws SSLException If any of required parameters is missing.
     */
    @Override protected void checkParameters() throws SSLException {
        assert keyStoreType != null;
        assert proto != null;

        checkNullParameter(keyStoreFilePath, "keyStoreFilePath");
        checkNullParameter(keyStorePwd, "keyStorePwd");

        if (trustMgrs == null) {
            if (trustStoreFilePath == null)
                throw new SSLException("Failed to initialize SSL context (either trustStoreFilePath or " +
                    "trustManagers must be provided)");
            else
                checkNullParameter(trustStorePwd, "trustStorePwd");
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + parameters();
    }
}
