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

package org.apache.ignite.internal.processors.ssl;

import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;

/** SSL context factory that the SSL factory registry can reload on request and before its certificates expire. */
public interface ReloadableSslFactory extends Factory<SSLContext> {
    /**
     * Loads the keys and trust settings again for new connections. If loading fails, new connections keep the previous ones.
     *
     * @throws IgniteException If loading fails.
     */
    public void init();

    /**
     * The automatic reload time is the earliest expiry among the certificate chains of the private keys that new connections
     * use, minus the lead time of the factory. A chain expires when any certificate in it expires.
     * Keys without an X.509 certificate chain are ignored, as TLS cannot use them. A factory that has lost the keys
     * of new connections, for example after a failed reload outside the registry, gives a time that has already come,
     * so that the next check reloads it.
     *
     * @return Automatic reload time in milliseconds since the epoch, or {@code 0} if the factory gives no private key with
     *     an X.509 certificate chain.
     * @throws IgniteException If the factory cannot give the certificates that new connections use.
     */
    public long autoReloadTime();

    /** @return Whether the registry reloads the factory before its certificates expire. Manual reload works either way. */
    public boolean isAutoReloadEnabled();

    /**
     * @param store Loaded key store.
     * @return Earliest certificate expiry in the private key chains, or {@code 0} if the store has no private key
     *     with an X.509 certificate chain.
     * @throws GeneralSecurityException If the key store cannot be read.
     */
    public static long keyChainsExpiry(KeyStore store) throws GeneralSecurityException {
        long earliest = Long.MAX_VALUE;

        for (Enumeration<String> aliases = store.aliases(); aliases.hasMoreElements(); ) {
            String alias = aliases.nextElement();

            if (!store.isKeyEntry(alias))
                continue;

            earliest = Math.min(earliest, chainExpiry(store.getCertificateChain(alias)));
        }

        return earliest == Long.MAX_VALUE ? 0 : earliest;
    }

    /**
     * @param chain Certificate chain of a key entry, or {@code null} if the entry has none.
     * @return Earliest expiry in the chain, or {@link Long#MAX_VALUE} if there is no chain or it holds a certificate other
     *     than X.509, as TLS ignores such keys.
     */
    private static long chainExpiry(Certificate[] chain) {
        long expiry = Long.MAX_VALUE;

        if (chain == null)
            return expiry;

        for (Certificate cert : chain) {
            if (!(cert instanceof X509Certificate x509Cert))
                return Long.MAX_VALUE;

            expiry = Math.min(expiry, x509Cert.getNotAfter().getTime());
        }

        return expiry;
    }
}
