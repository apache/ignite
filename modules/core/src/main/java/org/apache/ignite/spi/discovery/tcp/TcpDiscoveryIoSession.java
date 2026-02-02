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

package org.apache.ignite.spi.discovery.tcp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.security.cert.Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryIoSessionSerializer.CompositeInputStream;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Handles I/O operations between discovery nodes in the cluster. This class encapsulates the socket connection used
 * by the {@link TcpDiscoverySpi} to exchange discovery protocol messages between nodes.
 * Ьessage serialization is delegated to a {@link TcpDiscoveryIoSessionSerializer}.
 */
public class TcpDiscoveryIoSession {
    /** Default size of buffer used for buffering socket in/out. */
    private static final int DFLT_SOCK_BUFFER_SIZE = 8192;

    /** */
    private final TcpDiscoverySpi spi;

    /** */
    private final Socket sock;

    /** Buffered socket output stream. */
    private final OutputStream out;

    /** Buffered socket input stream. */
    private final CompositeInputStream in;

    /** */
    private final TcpDiscoveryIoSessionSerializer serde;

    /**
     * Creates a new discovery I/O session bound to the given socket.
     *
     * @param sock Socket connected to a remote discovery node.
     * @param spi  Discovery SPI instance owning this session.
     * @param serde Session serializer.
     * @throws IgniteException If an I/O error occurs while initializing buffers.
     */
    TcpDiscoveryIoSession(Socket sock, TcpDiscoverySpi spi, TcpDiscoveryIoSessionSerializer serde) {
        this.sock = sock;
        this.spi = spi;
        this.serde = serde;

        try {
            int sendBufSize = sock.getSendBufferSize() > 0 ? sock.getSendBufferSize() : DFLT_SOCK_BUFFER_SIZE;
            int rcvBufSize = sock.getReceiveBufferSize() > 0 ? sock.getReceiveBufferSize() : DFLT_SOCK_BUFFER_SIZE;

            out = new BufferedOutputStream(sock.getOutputStream(), sendBufSize);
            in = new CompositeInputStream(new BufferedInputStream(sock.getInputStream(), rcvBufSize));
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Writes a discovery message to the underlying socket output stream.
     *
     * @param msg Message to send to the remote node.
     * @throws IgniteCheckedException If serialization fails.
     */
    void writeMessage(TcpDiscoveryAbstractMessage msg) throws IgniteCheckedException, IOException {
        serde.writeMessage(msg, out);
    }

    /**
     * Reads the next discovery message from the socket input stream.
     *
     * @param <T> Type of the expected message.
     * @return Deserialized message instance.
     * @throws IgniteCheckedException If deserialization fails.
     */
    <T> T readMessage() throws IgniteCheckedException, IOException {
        return serde.readMessage(in);
    }

    /** @return SSL certificate this session is established with. {@code null} if SSL is disabled or certificate validation failed. */
    @Nullable Certificate[] extractCertificates() {
        if (!spi.isSslEnabled())
            return null;

        try {
            return ((SSLSocket)sock).getSession().getPeerCertificates();
        }
        catch (SSLPeerUnverifiedException e) {
            U.error(spi.log, "Failed to extract discovery IO session certificates", e);

            return null;
        }
    }

    /** @return Socket. */
    public Socket socket() {
        return sock;
    }
}

