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
import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.Nullable;

/** Certificate dates and descriptions used by SSL reload, renewal and status. */
public class SslCertificates {
    /** */
    private SslCertificates() {
        // No-op.
    }

    /**
     * @param chain Chain, own certificate first.
     * @return Earliest expiry in the chain, in milliseconds since the epoch.
     */
    public static long chainNotAfter(X509Certificate[] chain) {
        long res = Long.MAX_VALUE;

        for (X509Certificate cert : chain)
            res = Math.min(res, cert.getNotAfter().getTime());

        return res;
    }

    /**
     * @param chain Chain.
     * @param time Time to check, in milliseconds since the epoch.
     * @return First certificate of the chain that is not valid at that time, {@code null} if all are.
     */
    public static @Nullable X509Certificate invalidAt(X509Certificate[] chain, long time) {
        for (X509Certificate cert : chain) {
            if (time < cert.getNotBefore().getTime() || time > cert.getNotAfter().getTime())
                return cert;
        }

        return null;
    }

    /**
     * @param cert Certificate, {@code null} if unknown.
     * @return Subject, issuer, serial and validity of the certificate; an empty string if it is unknown.
     */
    public static String describe(@Nullable X509Certificate cert) {
        return cert == null ? "" : "subject=" + cert.getSubjectX500Principal() + ", issuer=" + cert.getIssuerX500Principal() +
            ", serial=" + cert.getSerialNumber().toString(16) + ", notBefore=" + cert.getNotBefore().toInstant() +
            ", notAfter=" + cert.getNotAfter().toInstant();
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
