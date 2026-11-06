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

package org.apache.ignite.internal.ssl;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.ssl.SslBundle;
import org.jetbrains.annotations.Nullable;

/** Checks X.509 chain dates and formats certificates and failure reasons as message text. */
public class SslCertificates {
    /** */
    private SslCertificates() {
        // No-op.
    }

    /**
     * @param chain Chain, own certificate first.
     * @return Earliest expiry in the chain, in milliseconds since the epoch.
     */
    public static long chainNotAfter(Iterable<X509Certificate> chain) {
        long res = Long.MAX_VALUE;

        for (X509Certificate cert : chain)
            res = Math.min(res, cert.getNotAfter().getTime());

        return res;
    }

    /**
     * Chooses the first chain to expire, using the earliest leaf validity start to break ties.
     *
     * @param bundle Context and its chains.
     * @return Chain that determines renewal, or {@code null} if certificate metadata is unknown or contains no chains.
     */
    public static @Nullable List<X509Certificate> renewalChain(SslBundle bundle) {
        Map<String, List<X509Certificate>> chains = bundle.certificateChains();

        if (chains == null || chains.isEmpty())
            return null;

        List<X509Certificate> earliest = null;
        long earliestExpiry = Long.MAX_VALUE;

        for (List<X509Certificate> chain : chains.values()) {
            long expiry = chainNotAfter(chain);

            if (earliest == null || expiry < earliestExpiry ||
                expiry == earliestExpiry && chain.get(0).getNotBefore().before(earliest.get(0).getNotBefore())) {
                earliest = chain;
                earliestExpiry = expiry;
            }
        }

        return earliest;
    }

    /**
     * @param chain Chain.
     * @param time Time to check, in milliseconds since the epoch.
     * @return First certificate of the chain that is not valid at that time, {@code null} if all are.
     */
    public static @Nullable X509Certificate invalidAt(Iterable<X509Certificate> chain, long time) {
        for (X509Certificate cert : chain) {
            if (time < cert.getNotBefore().getTime() || time > cert.getNotAfter().getTime())
                return cert;
        }

        return null;
    }

    /**
     * @param cert Certificate to describe.
     * @return Subject, issuer, serial number and validity period.
     */
    public static String describe(X509Certificate cert) {
        return "subject=" + cert.getSubjectX500Principal() + ", issuer=" + cert.getIssuerX500Principal() +
            ", serial=" + cert.getSerialNumber().toString(16) + ", notBefore=" + cert.getNotBefore().toInstant() +
            ", notAfter=" + cert.getNotAfter().toInstant();
    }

    /**
     * @param chains Nonempty chains by key alias, or {@code null} when metadata is unavailable.
     * @return Description of every alias and its leaf certificate, or an empty string if the aliases are unknown.
     */
    public static String describeChains(@Nullable Map<String, List<X509Certificate>> chains) {
        if (chains == null)
            return "";

        List<String> descriptions = new ArrayList<>();

        chains.forEach((alias, chain) -> descriptions.add("alias=" + alias + ", " + describe(chain.get(0))));

        return String.join("; ", descriptions);
    }

    /**
     * @param e Failure.
     * @return Non-repeated messages from the exception and its causes, or {@link Throwable#toString()} if none are available.
     */
    public static String reason(Throwable e) {
        StringBuilder sb = new StringBuilder();

        for (Throwable t : X.getThrowableList(e)) {
            String msg = t.getMessage();

            boolean wrapsCauseOnly = t.getCause() != null && t.getCause().toString().equals(msg);

            if (msg == null || msg.isEmpty() || wrapsCauseOnly || sb.indexOf(msg) >= 0)
                continue;

            if (!sb.isEmpty())
                sb.append(": ");

            sb.append(msg);
        }

        return !sb.isEmpty() ? sb.toString() : e.toString();
    }
}
