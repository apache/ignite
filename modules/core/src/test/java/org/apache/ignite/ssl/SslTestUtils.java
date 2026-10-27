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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

/** Probes the certificates that TLS servers present. */
public class SslTestUtils {
    /** Timeout for a probe connection or TLS handshake. */
    private static final int SOCKET_TIMEOUT = 30_000;

    /** */
    private SslTestUtils() {
        // No-op.
    }

    /**
     * @param probe Context to connect with.
     * @param port Loopback port to connect to.
     * @return Certificate the server presents on a new TLS connection to that port.
     */
    public static X509Certificate servedCertificate(SSLContext probe, int port) throws Exception {
        try (SSLSocket sock = (SSLSocket)probe.getSocketFactory().createSocket()) {
            sock.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), SOCKET_TIMEOUT);
            sock.setSoTimeout(SOCKET_TIMEOUT);

            sock.startHandshake();

            return (X509Certificate)sock.getSession().getPeerCertificates()[0];
        }
    }
}
