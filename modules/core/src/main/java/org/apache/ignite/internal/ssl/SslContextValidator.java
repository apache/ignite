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
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;

/**
 * Runs a TLS handshake in memory to tell whether an SSL context can still serve connections between nodes.
 * <p>
 * Only a refused handshake counts as a failure. An exchange that cannot be driven to completion for any other
 * reason lets the context through: the check is here to catch a certificate that would break the cluster, not to
 * become a new way of blocking a certificate rotation.
 */
public class SslContextValidator {
    /** Bound on handshake steps, so that an unexpected engine state cannot spin here forever. */
    private static final int MAX_STEPS = 100;

    /** */
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    /** */
    private SslContextValidator() {
        // No-op.
    }

    /**
     * Checks the context the way an inter-node transport would use it: both ends of such a transport run the same
     * configuration, so a context that cannot handshake against itself cannot serve new connections between nodes.
     * Client-facing transports must not be checked this way, as a client legitimately holds a different key and
     * trust store.
     *
     * @param ctx SSL context to check.
     * @throws SSLException If the handshake was refused. The caller must keep the context currently in use.
     */
    public static void validateInterNode(SSLContext ctx) throws SSLException {
        SSLEngine srv = ctx.createSSLEngine();

        srv.setUseClientMode(false);
        srv.setNeedClientAuth(true);

        SSLEngine cli = ctx.createSSLEngine();

        cli.setUseClientMode(true);

        SSLSession ses = cli.getSession();

        ByteBuffer cliNet = flipped(ses.getPacketBufferSize());
        ByteBuffer srvNet = flipped(ses.getPacketBufferSize());
        ByteBuffer app = ByteBuffer.allocate(ses.getApplicationBufferSize());

        cli.beginHandshake();
        srv.beginHandshake();

        for (int i = 0; i < MAX_STEPS; i++) {
            boolean progress = step(cli, cliNet, srvNet, app) | step(srv, srvNet, cliNet, app);

            if (done(cli) && done(srv))
                return;

            if (!progress)
                return;
        }
    }

    /**
     * @param engine Engine to advance by a single handshake step.
     * @param out Buffer the engine writes its handshake data to.
     * @param in Buffer holding the data written by the peer engine.
     * @param app Scratch buffer for decoded data.
     * @return {@code True} if the engine moved forward.
     * @throws SSLException If the handshake was refused.
     */
    private static boolean step(SSLEngine engine, ByteBuffer out, ByteBuffer in, ByteBuffer app)
        throws SSLException {
        switch (engine.getHandshakeStatus()) {
            case NEED_TASK:
                Runnable task;

                while ((task = engine.getDelegatedTask()) != null)
                    task.run();

                return true;

            case NEED_WRAP:
                // The peer has not read the previous flight yet, let it run first.
                if (out.hasRemaining())
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

    /**
     * @param engine Engine to check.
     * @return {@code True} if the engine has nothing left to exchange.
     */
    private static boolean done(SSLEngine engine) {
        return engine.getHandshakeStatus() == NOT_HANDSHAKING || engine.getHandshakeStatus() == FINISHED;
    }

    /**
     * @param cap Buffer capacity.
     * @return Empty buffer ready to be filled by {@link SSLEngine#wrap}.
     */
    private static ByteBuffer flipped(int cap) {
        ByteBuffer buf = ByteBuffer.allocate(cap);

        buf.flip();

        return buf;
    }
}
