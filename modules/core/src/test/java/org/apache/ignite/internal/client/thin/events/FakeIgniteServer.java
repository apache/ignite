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

package org.apache.ignite.internal.client.thin.events;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.EnumSet;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryByteBufferInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.client.thin.ProtocolContext;
import org.apache.ignite.internal.client.thin.ProtocolVersion;
import org.apache.ignite.internal.client.thin.ProtocolVersionFeature;
import org.apache.ignite.internal.client.thin.io.gridnioserver.GridNioClientParser;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.util.nio.GridNioCodecFilter;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.testframework.junits.JUnitAssertAware;
import org.jetbrains.annotations.Nullable;

/**
 * A fake ignite server for testing handshake and connection errors handling on the thin client side.
 */
public class FakeIgniteServer extends JUnitAssertAware implements GridNioServerListener<ByteBuffer>, AutoCloseable {
    /** */
    static final int HANDSHAKE_PERFORMED = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    public static final byte[] EMPTY_BYTES = new byte[0];

    /** */
    private final GridNioServer<ByteBuffer> srv;

    /** */
    private final EnumSet<ErrorType> errorTypes;

    /** */
    private final ProtocolVersion protoVer;

    /** */
    private final UUID nodeId = UUID.randomUUID();

    /** */
    public FakeIgniteServer(InetAddress addr, int port, IgniteLogger logger) {
        this(addr, port, logger, null, null);
    }

    /** */
    public FakeIgniteServer(InetAddress addr, int port, IgniteLogger logger, EnumSet<ErrorType> errorTypes) {
        this(addr, port, logger, null, errorTypes);
    }

    /** */
    public FakeIgniteServer(InetAddress addr, int port, IgniteLogger logger, ProtocolVersion protoVer) {
        this(addr, port, logger, protoVer, null);
    }

    /** */
    public FakeIgniteServer(
        InetAddress addr,
        int port,
        IgniteLogger logger,
        ProtocolVersion protoVer,
        EnumSet<ErrorType> errorTypes
    ) {
        this.protoVer = protoVer != null ? protoVer : ProtocolVersion.V1_7_0;

        this.errorTypes = errorTypes != null ? errorTypes : EnumSet.noneOf(ErrorType.class);

        try {
            srv = GridNioServer.<ByteBuffer>builder()
                .address(addr)
                .port(port)
                .listener(this)
                .logger(logger)
                .selectorCount(1)
                .byteOrder(ByteOrder.LITTLE_ENDIAN)
                .directBuffer(true)
                .directMode(false)
                .filters(
                    new GridNioCodecFilter(new GridNioClientParser(), logger, false)
                )
                .build();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to initialize fake server", e);
        }
    }

    /** */
    public void start() throws IgniteCheckedException {
        srv.start();
    }

    /** */
    public void stop() {
        srv.stop();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        srv.stop();
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {

    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {

    }

    /** {@inheritDoc} */
    @Override public void onMessageSent(GridNioSession ses, ByteBuffer msg) {

    }

    /** {@inheritDoc} */
    @Override public void onMessage(GridNioSession ses, ByteBuffer msg) {
        if (ses.meta(HANDSHAKE_PERFORMED) == null) {
            if (errorTypes.contains(ErrorType.HANDSHAKE_CONNECTION_ERROR)) {
                ses.close();

                return;
            }

            BinaryInputStream res = BinaryByteBufferInputStream.create(msg);
            try (BinaryReaderExImpl reader = new BinaryReaderExImpl(null, res, null, null, true, true)) {
                byte reqType = reader.readByte();

                assertEquals(ClientListenerRequest.HANDSHAKE, reqType);

                ProtocolVersion clientVer = new ProtocolVersion(reader.readShort(), reader.readShort(), reader.readShort());

                ByteBuffer msgRes = createMessage(writer -> {
                    if (errorTypes.contains(ErrorType.HANDSHAKE_ERROR) || errorTypes.contains(ErrorType.AUTHENTICATION_ERROR)
                        || protoVer.compareTo(clientVer) != 0) {
                        writer.writeBoolean(false);

                        writer.writeShort(protoVer.major());
                        writer.writeShort(protoVer.minor());
                        writer.writeShort(protoVer.patch());

                        if (errorTypes.contains(ErrorType.AUTHENTICATION_ERROR)) {
                            writer.writeString("Authentication failed");
                            writer.writeInt(ClientStatus.AUTH_FAILED);
                        }
                        else {
                            writer.writeString("Handshake failed");
                            writer.writeInt(ClientStatus.FAILED);
                        }
                    }
                    else {
                        writer.writeBoolean(true);

                        if (ProtocolContext.isFeatureSupported(protoVer, ProtocolVersionFeature.BITMAP_FEATURES))
                            writer.writeByteArray(EMPTY_BYTES);

                        if (ProtocolContext.isFeatureSupported(protoVer, ProtocolVersionFeature.PARTITION_AWARENESS))
                            writer.writeUuid(nodeId);

                        ses.addMeta(HANDSHAKE_PERFORMED, true);
                    }
                });

                ses.send(msgRes);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            if (errorTypes.contains(ErrorType.CONNECTION_ERROR))
                ses.close();
        }
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) {

    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) {

    }

    /** {@inheritDoc} */
    @Override public void onFailure(FailureType failureType, Throwable failure) {

    }

    /** */
    private ByteBuffer createMessage(Consumer<BinaryRawWriter> writerAction) {
        try (BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(32), null, null)) {
            writer.writeInt(0);

            writerAction.accept(writer);

            writer.out().writeInt(0, writer.out().position() - 4); // actual size

            return ByteBuffer.wrap(writer.out().arrayCopy(), 0, writer.out().position());
        }
    }

    /**
     * @return Fake node id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * Injected error types.
     */
    public enum ErrorType {
        /** Generic handshake error. */
        HANDSHAKE_ERROR,

        /** Connection error on handshake. */
        HANDSHAKE_CONNECTION_ERROR,

        /** Authentication error. */
        AUTHENTICATION_ERROR,

        /** Connection error. */
        CONNECTION_ERROR
    }
}
