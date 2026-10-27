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
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * A loaded key store and the settings needed to create its key managers. Both managers and certificate metadata are read from
 * this store without loading it again. Do not modify the store while any context created from it is in use: key managers
 * such as PKIX read it during handshakes. Load replacement material into a separate store.
 */
public final class KeyMaterial {
    /** */
    private final KeyStore store;

    /** */
    private final char[] keyPwd;

    /** */
    private final String algorithm;

    /**
     * @param store Loaded key store, never {@code null}. For a context without private keys, initialize an empty store with
     *      {@code KeyStore.load(null, null)}.
     * @param keyPwd Password for its private keys, or {@code null} if the store requires no password.
     * @param algorithm Key manager algorithm.
     */
    public KeyMaterial(KeyStore store, @Nullable char[] keyPwd, String algorithm) {
        A.notNull(store, "store", algorithm, "algorithm");

        this.store = store;
        this.keyPwd = keyPwd == null ? null : keyPwd.clone();
        this.algorithm = algorithm;
    }

    /**
     * @return Key managers initialized from the loaded store.
     * @throws SSLException If the algorithm is unavailable or the keys cannot be read.
     */
    public KeyManager[] keyManagers() throws SSLException {
        try {
            KeyManagerFactory factory = KeyManagerFactory.getInstance(algorithm);

            factory.init(store, keyPwd);

            return factory.getKeyManagers();
        }
        catch (NoSuchAlgorithmException e) {
            throw new SSLException("Unsupported key algorithm: " + algorithm, e);
        }
        catch (GeneralSecurityException e) {
            throw new SSLException("Failed to initialize Key Manager (security exception occurred)", e);
        }
    }

    /**
     * @return Immutable X.509 chains by private-key alias, own certificate first. An empty map means the store lists no private
     *      keys. JKS lists a private key stored without a chain as a secret key, so such a key is not included. An empty list
     *      retains an alias whose chain is absent or contains a non-X.509 certificate; key managers never present such a key.
     * @throws SSLException If the loaded store cannot be read.
     */
    public Map<String, List<X509Certificate>> certificateChains() throws SSLException {
        try {
            Map<String, List<X509Certificate>> chains = new TreeMap<>();

            for (Enumeration<String> aliases = store.aliases(); aliases.hasMoreElements(); ) {
                String alias = aliases.nextElement();

                if (store.entryInstanceOf(alias, KeyStore.PrivateKeyEntry.class))
                    chains.put(alias, certificateChain(store.getCertificateChain(alias)));
            }

            return Collections.unmodifiableMap(chains);
        }
        catch (KeyStoreException e) {
            throw new SSLException("Failed to read certificate chains from key store", e);
        }
    }

    /**
     * @param certs Chain from the store, if present.
     * @return Immutable X.509 chain, or an empty list if it is absent or contains another certificate type.
     */
    private static List<X509Certificate> certificateChain(@Nullable Certificate[] certs) {
        if (certs == null)
            return Collections.emptyList();

        List<X509Certificate> chain = new ArrayList<>(certs.length);

        for (Certificate cert : certs) {
            if (!(cert instanceof X509Certificate))
                return Collections.emptyList();

            chain.add((X509Certificate)cert);
        }

        return Collections.unmodifiableList(chain);
    }
}
