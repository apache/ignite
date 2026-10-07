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
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import org.apache.ignite.ssl.SslContextFactory;
import org.jetbrains.annotations.Nullable;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;

/** Tells what an SSL context presents and whether nodes accept it, by TLS handshakes run in memory, and describes certificates. */
public class SslCertificates {
    /** Bound on handshake steps, so that an unexpected engine state cannot spin here forever. */
    private static final int MAX_STEPS = 100;

    /** */
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    /** */
    private SslCertificates() {
        // No-op.
    }

    /**
     * Checks the context the way an inter-node transport uses it. Both ends run the same configuration, so a context that cannot handshake
     * with itself cannot serve new connections between nodes. Only a refused handshake counts. An exchange that cannot be driven to the end
     * for another reason lets the context through, so that the check never blocks a rotation by itself.
     *
     * @param ctx SSL context to check.
     * @throws SSLException If the handshake was refused.
     */
    public static void validateInterNode(SSLContext ctx) throws SSLException {
        handshake(ctx, ctx, true);
    }

    /**
     * @param ctx SSL context.
     * @return Chain the context presents to a client that trusts anything, own certificate first, or {@code null} if it is unknown.
     */
    public static @Nullable X509Certificate[] servedChain(SSLContext ctx) {
        try {
            SSLContext probe = SSLContext.getInstance("TLS");

            probe.init(null, new TrustManager[] {SslContextFactory.getDisabledTrustManager()}, null);

            SSLSession ses = handshake(probe, ctx, false);

            if (ses == null)
                return null;

            Certificate[] certs = ses.getPeerCertificates();

            return Arrays.copyOf(certs, certs.length, X509Certificate[].class);
        }
        catch (Exception cannotTell) {
            return null;
        }
    }

    /**
     * @param chain Chain, own certificate first.
     * @return The earliest time a certificate in the chain expires: peers refuse the chain from then on.
     */
    public static long chainNotAfter(X509Certificate[] chain) {
        long res = Long.MAX_VALUE;

        for (X509Certificate cert : chain)
            res = Math.min(res, cert.getNotAfter().getTime());

        return res;
    }

    /**
     * @param chain Chain.
     * @param time Time.
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
     * @return The certificate as the node log, the commands and the errors name it; an empty string if it is unknown.
     */
    public static String describe(@Nullable X509Certificate cert) {
        return cert == null ? "" : "subject=" + cert.getSubjectX500Principal() + ", issuer=" + cert.getIssuerX500Principal() +
            ", serial=" + cert.getSerialNumber().toString(16) + ", notBefore=" + cert.getNotBefore().toInstant() +
            ", notAfter=" + cert.getNotAfter().toInstant();
    }

    /**
     * @param e Failure.
     * @return Messages along its chain of causes, each once; a failure out of a user-supplied factory may carry none and is then named by
     *      its type.
     */
    public static String reason(Throwable e) {
        StringBuilder sb = new StringBuilder();

        int depth = 0;

        for (Throwable t = e; t != null && depth < 10; t = t.getCause(), depth++) {
            String msg = t.getMessage();

            boolean wrapsCauseOnly = t.getCause() != null && t.getCause().toString().equals(msg);

            if (msg == null || msg.isEmpty() || wrapsCauseOnly || sb.indexOf(msg) >= 0)
                continue;

            if (sb.length() > 0)
                sb.append(": ");

            sb.append(msg);
        }

        return sb.length() > 0 ? sb.toString() : e.toString();
    }

    /**
     * @param cliCtx Context of the client end.
     * @param srvCtx Context of the server end.
     * @param needClientAuth Whether the server end demands a client certificate.
     * @return Session of the client end if the handshake completed, {@code null} if it could not be driven to the end.
     * @throws SSLException If either side refused the handshake.
     */
    private static @Nullable SSLSession handshake(SSLContext cliCtx, SSLContext srvCtx, boolean needClientAuth) throws SSLException {
        SSLEngine cli = cliCtx.createSSLEngine();
        SSLEngine srv = srvCtx.createSSLEngine();

        cli.setUseClientMode(true);
        srv.setUseClientMode(false);
        srv.setNeedClientAuth(needClientAuth);

        int bufSize = cli.getSession().getPacketBufferSize();

        ByteBuffer cliNet = ByteBuffer.allocate(bufSize).limit(0);
        ByteBuffer srvNet = ByteBuffer.allocate(bufSize).limit(0);
        ByteBuffer app = ByteBuffer.allocate(cli.getSession().getApplicationBufferSize());

        cli.beginHandshake();
        srv.beginHandshake();

        for (int i = 0; i < MAX_STEPS; i++) {
            boolean progress = step(cli, cliNet, srvNet, app) | step(srv, srvNet, cliNet, app);

            if (done(cli) && done(srv))
                return cli.getSession();

            if (!progress)
                return null;
        }

        return null;
    }

    /**
     * @param engine Engine to advance by a single handshake step.
     * @param out Buffer the engine writes its handshake data to.
     * @param in Buffer holding the data written by the peer engine.
     * @param app Scratch buffer for decoded data.
     * @return {@code True} if the engine moved forward.
     * @throws SSLException If the handshake was refused.
     */
    private static boolean step(SSLEngine engine, ByteBuffer out, ByteBuffer in, ByteBuffer app) throws SSLException {
        switch (engine.getHandshakeStatus()) {
            case NEED_TASK:
                Runnable task;

                while ((task = engine.getDelegatedTask()) != null)
                    task.run();

                return true;

            case NEED_WRAP:
                boolean peerReadPrevious = !out.hasRemaining();

                if (!peerReadPrevious)
                    return false;

                out.clear();

                engine.wrap(EMPTY, out);

                out.flip();

                return true;

            case NEED_UNWRAP:
            case NEED_UNWRAP_AGAIN:
                if (!in.hasRemaining())
                    return false;

                app.clear();

                engine.unwrap(in, app);

                return true;

            default:
                return false;
        }
    }

    /** */
    private static boolean done(SSLEngine engine) {
        return engine.getHandshakeStatus() == NOT_HANDSHAKING || engine.getHandshakeStatus() == FINISHED;
    }
}
