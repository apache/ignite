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

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/** Describes certificates the way the node log, the reload and status commands and the metrics name them. */
public class SslCertificates {
    /** */
    private SslCertificates() {
        // No-op.
    }

    /**
     * @param cert Certificate to describe, {@code null} if unknown.
     * @return The certificate as log lines and errors name it, or an empty string if it is unknown.
     */
    public static String describe(@Nullable X509Certificate cert) {
        return cert == null ? "" : "subject=" + cert.getSubjectX500Principal() +
            ", issuer=" + cert.getIssuerX500Principal() +
            ", serial=" + serial(cert) +
            ", notBefore=" + cert.getNotBefore().toInstant() +
            ", notAfter=" + cert.getNotAfter().toInstant();
    }

    /**
     * @param cert Certificate.
     * @return Its serial number in hexadecimal, the way certificate authorities list it.
     */
    public static String serial(X509Certificate cert) {
        return cert.getSerialNumber().toString(16);
    }

    /**
     * @param cert Certificate.
     * @return Its subject alternative names as {@code TYPE:value}, comma-separated, or an empty string if it has none.
     */
    public static String subjectAlternativeNames(X509Certificate cert) {
        Collection<List<?>> names;

        try {
            names = cert.getSubjectAlternativeNames();
        }
        catch (CertificateParsingException ignored) {
            return "";
        }

        if (names == null)
            return "";

        List<String> res = new ArrayList<>();

        for (List<?> name : names)
            res.add(sanType((Integer)name.get(0)) + ':' + name.get(1));

        return String.join(", ", res);
    }

    /**
     * @param chain Chain, its own certificate first.
     * @return The earliest time any certificate in the chain stops being valid: a peer refuses the chain from then on,
     *      whatever the own certificate says.
     */
    public static long chainNotAfter(X509Certificate[] chain) {
        long res = Long.MAX_VALUE;

        for (X509Certificate cert : chain)
            res = Math.min(res, cert.getNotAfter().getTime());

        return res;
    }

    /**
     * @param certs Authorities.
     * @return Them as {@code subject until date}, semicolon-separated.
     */
    public static String authorities(List<X509Certificate> certs) {
        List<String> res = new ArrayList<>();

        for (X509Certificate cert : certs)
            res.add(cert.getSubjectX500Principal() + " until " + date(cert.getNotAfter().getTime()));

        return String.join("; ", res);
    }

    /**
     * @param time Time in milliseconds.
     * @return Its date in UTC.
     */
    public static String date(long time) {
        return Instant.ofEpochMilli(time).atOffset(ZoneOffset.UTC).toLocalDate().toString();
    }

    /**
     * @param type Subject alternative name type, as RFC 5280 numbers it.
     * @return Its usual short name.
     */
    private static String sanType(int type) {
        switch (type) {
            case 1: return "EMAIL";
            case 2: return "DNS";
            case 6: return "URI";
            case 7: return "IP";
            default: return String.valueOf(type);
        }
    }
}
