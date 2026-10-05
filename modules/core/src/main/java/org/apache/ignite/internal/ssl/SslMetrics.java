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
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Metrics of the certificates one transport serves: what it presents, which authorities it trusts, and how its
 * reloads went. One registry per transport, read from the component that serves it.
 */
public class SslMetrics {
    /** Prefix of the registries. */
    public static final String SSL_METRICS = "ssl";

    /** */
    private SslMetrics() {
        // No-op.
    }

    /**
     * @param user Transport, one of the names in {@link SslContextReloadable}.
     * @return Name of its registry.
     */
    public static String registryName(String user) {
        switch (user) {
            case SslContextReloadable.CLIENT_CONNECTOR: return metricName(SSL_METRICS, "client", "connector");
            case SslContextReloadable.BINARY_REST: return metricName(SSL_METRICS, "rest", "binary");
            case SslContextReloadable.HTTP_REST: return metricName(SSL_METRICS, "rest", "http");
            default: return metricName(SSL_METRICS, user);
        }
    }

    /**
     * @param mgr Metric manager of the node.
     * @param user Transport, one of the names in {@link SslContextReloadable}.
     * @param comp Component that serves it.
     */
    public static void register(GridMetricManager mgr, String user, SslContextReloadable comp) {
        MetricRegistryImpl reg = mgr.registry(registryName(user));

        reg.register("CertificateSubject",
            () -> {
                X509Certificate cert = comp.servedCertificate();

                return cert == null ? null : cert.getSubjectX500Principal().toString();
            },
            String.class, "Subject DN of the certificate presented on new connections.");

        reg.register("CertificateIssuer",
            () -> {
                X509Certificate cert = comp.servedCertificate();

                return cert == null ? null : cert.getIssuerX500Principal().toString();
            },
            String.class, "Issuer DN of the certificate presented on new connections.");

        reg.register("CertificateSerialNumber",
            () -> {
                X509Certificate cert = comp.servedCertificate();

                return cert == null ? null : SslCertificates.serial(cert);
            },
            String.class, "Serial number of the certificate presented on new connections, in hexadecimal.");

        reg.register("CertificateSubjectAlternativeNames",
            () -> {
                X509Certificate cert = comp.servedCertificate();

                return cert == null ? null : SslCertificates.subjectAlternativeNames(cert);
            },
            String.class, "Subject alternative names of the certificate presented on new connections.");

        reg.register("CertificateNotBefore",
            () -> {
                X509Certificate cert = comp.servedCertificate();

                return cert == null ? 0 : cert.getNotBefore().getTime();
            },
            "Time the certificate presented on new connections becomes valid, in milliseconds; 0 if unknown.");

        reg.register("CertificateNotAfter",
            () -> {
                X509Certificate cert = comp.servedCertificate();

                return cert == null ? 0 : cert.getNotAfter().getTime();
            },
            "Expiry time of the certificate presented on new connections, in milliseconds; 0 if unknown.");

        reg.register("CertificateChainNotAfter",
            () -> {
                X509Certificate[] chain = comp.servedChain();

                return chain == null ? 0 : SslCertificates.chainNotAfter(chain);
            },
            "Earliest expiry time in the chain presented on new connections, in milliseconds; 0 if unknown.");

        reg.register("TrustedAuthorities",
            () -> SslCertificates.authorities(comp.trustedAuthorities()),
            String.class, "Authorities in the trust store, with their expiry dates; empty if they cannot be read.");

        reg.register("LastReloadTime",
            () -> comp.reloadState().lastSuccessTime(),
            "Time of the last successful reload of the certificates, in milliseconds; 0 if there was none.");

        reg.register("LastReloadFailureTime",
            () -> comp.reloadState().lastFailureTime(),
            "Time of the last failed reload since the last successful one, in milliseconds; 0 if there was none.");

        reg.register("LastReloadFailure",
            () -> comp.reloadState().lastFailure(),
            String.class, "Reason of the last failed reload since the last successful one.");

        reg.register("ReloadFailures",
            () -> comp.reloadState().failures(),
            "Failed reloads in a row since the last successful one.");
    }
}
