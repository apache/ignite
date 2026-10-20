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

import java.nio.ByteBuffer;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.security.auth.x500.X500Principal;
import org.apache.ignite.ssl.SSLContextWrapper;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.HOURS;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_WRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
import static javax.net.ssl.SSLEngineResult.Status.CLOSED;
import static javax.net.ssl.SSLEngineResult.Status.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests the in-memory TLS probes used to inspect and validate reloaded contexts. */
public class SslCertificatesTest {
    /** A certificate request must reveal authorities without requiring the probe to present a client certificate. */
    @Test
    public void testTrustedAuthoritiesWithOptionalClientAuthentication() throws Exception {
        TestCertificateAuthority ca = new TestCertificateAuthority("authority");

        long now = System.currentTimeMillis();

        SSLContext ctx = TestCertificateAuthority.context(ca.issue("node", now - HOURS.toMillis(1), now + HOURS.toMillis(1)),
            ca.trustStore());

        for (String proto : new String[] {"TLSv1.2", "TLSv1.3"}) {
            SSLParameters params = new SSLParameters();

            params.setProtocols(new String[] {proto});

            assertEquals(proto, List.of(new X500Principal("CN=authority")),
                SslCertificates.trustedAuthorities(new SSLContextWrapper(ctx, params)));
        }
    }

    /** Two engines waiting for data must not be accepted as a successful validation handshake. */
    @Test
    public void testStalledHandshakeRefused() throws Exception {
        SSLContext ctx = mock(SSLContext.class);
        SSLEngine engine = mock(SSLEngine.class);

        when(ctx.createSSLEngine()).thenReturn(engine);
        when(engine.getSession()).thenReturn(SSLContext.getDefault().createSSLEngine().getSession());
        when(engine.getHandshakeStatus()).thenReturn(NEED_UNWRAP);

        assertThrows(SSLException.class, () -> SslCertificates.checkInterNodeHandshake(ctx));
        assertThrows(SSLException.class, () -> SslCertificates.checkAccepts(ctx, ctx));
    }

    /** A wrap that consumes and produces nothing without changing state must not be counted as progress. */
    @Test
    public void testWrapWithoutProgressRefused() throws Exception {
        SSLContext ctx = mock(SSLContext.class);
        SSLEngine engine = mock(SSLEngine.class);

        when(ctx.createSSLEngine()).thenReturn(engine);
        when(engine.getSession()).thenReturn(SSLContext.getDefault().createSSLEngine().getSession());
        when(engine.getHandshakeStatus()).thenReturn(NEED_WRAP);
        when(engine.wrap(any(ByteBuffer.class), any(ByteBuffer.class)))
            .thenReturn(new SSLEngineResult(OK, NEED_WRAP, 0, 0));

        assertThrows(SSLException.class, () -> SslCertificates.checkInterNodeHandshake(ctx));
        assertThrows(SSLException.class, () -> SslCertificates.checkAccepts(ctx, ctx));
    }

    /** An engine closing during a wrap must fail validation even if it is no longer handshaking. */
    @Test
    public void testClosedHandshakeRefused() throws Exception {
        SSLContext ctx = mock(SSLContext.class);
        SSLEngine engine = mock(SSLEngine.class);

        when(ctx.createSSLEngine()).thenReturn(engine);
        when(engine.getSession()).thenReturn(SSLContext.getDefault().createSSLEngine().getSession());
        when(engine.getHandshakeStatus()).thenReturn(NEED_WRAP);
        when(engine.wrap(any(ByteBuffer.class), any(ByteBuffer.class)))
            .thenReturn(new SSLEngineResult(CLOSED, NOT_HANDSHAKING, 0, 0));

        assertThrows(SSLException.class, () -> SslCertificates.checkInterNodeHandshake(ctx));
        assertThrows(SSLException.class, () -> SslCertificates.checkAccepts(ctx, ctx));
    }
}
