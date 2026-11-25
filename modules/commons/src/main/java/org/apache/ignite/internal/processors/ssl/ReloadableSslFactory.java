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

import static java.util.concurrent.TimeUnit.DAYS;

/** SSL context factory that the SSL factory registry can reload on request and before its certificates expire. */
public interface ReloadableSslFactory extends Factory<SSLContext> {
    /**
     * Loads the keys and trust settings again for new connections. If loading fails, new connections keep the previous ones.
     *
     * @throws IgniteException If loading fails.
     */
    public void init();

    /**
     * The hot reload time comes from the certificate chains of the private keys that new connections use.
     * Each certificate in these chains becomes due when the time left before it expires drops to a third of its lifetime
     * or to 7 days, whichever is shorter.
     * The factory is due when its first certificate is due. Keys without an X.509 certificate chain are ignored,
     * as TLS cannot use them.
     *
     * @return Hot reload time in milliseconds since the epoch, or {@code 0} if the factory gives no private key with
     *     an X.509 certificate chain.
     * @throws IgniteException If the factory cannot give the certificates that new connections use.
     */
    public long getHotReloadTime();

    /** @return Whether the registry reloads the factory before its certificates expire. Manual reload works either way. */
    public default boolean isHotReloadEnabled() {
        return true;
    }

    /**
     * Calculates the hot reload time that {@link #getHotReloadTime()} describes.
     *
     * @param store Loaded key store.
     * @return Hot reload time in milliseconds since the epoch, or {@code 0} if the store has no private key
     *     with an X.509 certificate chain.
     * @throws GeneralSecurityException If the key store cannot be read.
     */
    public static long hotReloadTime(KeyStore store) throws GeneralSecurityException {
        long earliest = Long.MAX_VALUE;

        for (Enumeration<String> aliases = store.aliases(); aliases.hasMoreElements(); ) {
            String alias = aliases.nextElement();

            if (store.isKeyEntry(alias))
                earliest = Math.min(earliest, chainHotReloadTime(store.getCertificateChain(alias)));
        }

        return earliest == Long.MAX_VALUE ? 0 : earliest;
    }

    /**
     * @param chain Certificate chain of a key entry, or {@code null} if the entry has none.
     * @return Time when the first certificate in the chain is due, or {@link Long#MAX_VALUE} if there is no chain
     *     or it holds a certificate other than X.509, as TLS ignores such keys.
     */
    private static long chainHotReloadTime(Certificate[] chain) {
        long time = Long.MAX_VALUE;

        if (chain == null)
            return time;

        for (Certificate cert : chain) {
            if (!(cert instanceof X509Certificate x509))
                return Long.MAX_VALUE;

            long notAfter = x509.getNotAfter().getTime();
            long lifetime = notAfter - x509.getNotBefore().getTime();

            time = Math.min(time, notAfter - Math.min(DAYS.toMillis(7), lifetime / 3));
        }

        return time;
    }
}
