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

package org.apache.ignite.stream.socket;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridBufferedParser;
import org.apache.ignite.internal.util.nio.GridDelimitedParser;
import org.apache.ignite.internal.util.nio.GridNioCodecFilter;
import org.apache.ignite.internal.util.nio.GridNioFilter;
import org.apache.ignite.internal.util.nio.GridNioParser;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.stream.StreamAdapter;
import org.apache.ignite.stream.StreamTupleExtractor;
import org.jetbrains.annotations.Nullable;

/**
 * Server that receives data from TCP socket, converts it to key-value pairs using {@link StreamTupleExtractor} and
 * streams into {@link IgniteDataStreamer} instance.
 * <p>
 * By default server uses size-based message processing. That is every message sent over the socket is prepended with
 * 4-byte integer header containing message size. If message delimiter is defined (see {@link #setDelimiter}) then
 * delimiter-based message processing will be used. That is every message sent over the socket is appended with
 * provided delimiter.
 * <p>
 * Received messages through socket converts to Java object using standard serialization. Conversion functionality
 * can be customized via user defined {@link SocketMessageConverter} (e.g. in order to convert messages from
 * non Java clients).
 */
public class SocketStreamer<T, K, V> extends StreamAdapter<T, K, V> {
    /** Default threads. */
    private static final int DFLT_THREADS = Runtime.getRuntime().availableProcessors();

    /** Logger. */
    private IgniteLogger log;

    /** Address. */
    private InetAddress addr;

    /** Server port. */
    private int port;

    /** Threads number. */
    private int threads = DFLT_THREADS;

    /** Direct mode. */
    private boolean directMode;

    /** Delimiter. */
    private byte[] delim;

    /** Converter. */
    private SocketMessageConverter<T> converter;

    /** Server. */
    private GridNioServer<byte[]> srv;

    /**
     * Sets server address.
     *
     * @param addr Address.
     */
    public void setAddr(InetAddress addr) {
        this.addr = addr;
    }

    /**
     * Sets port number.
     *
     * @param port Port.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Sets threadds amount.
     *
     * @param threads Threads.
     */
    public void setThreads(int threads) {
        this.threads = threads;
    }

    /**
     * Sets direct mode flag.
     *
     * @param directMode Direct mode.
     */
    public void setDirectMode(boolean directMode) {
        this.directMode = directMode;
    }

    /**
     * Sets message delimiter.
     *
     * @param delim Delimiter.
     */
    public void setDelimiter(byte[] delim) {
        this.delim = delim;
    }

    /**
     * Sets message converter.
     *
     * @param converter Converter.
     */
    public void setConverter(SocketMessageConverter<T> converter) {
        this.converter = converter;
    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() {
        A.ensure(getSingleTupleExtractor() != null || getMultipleTupleExtractor() != null,
            "tupleExtractor (single or multiple)");
        A.notNull(getStreamer(), "streamer");
        A.notNull(getIgnite(), "ignite");
        A.ensure(threads > 0, "threads > 0");

        log = getIgnite().log();

        GridNioServerListener<byte[]> lsnr = new GridNioServerListenerAdapter<byte[]>() {
            @Override public void onConnected(GridNioSession ses) {
                assert ses.accepted();

                if (log.isDebugEnabled())
                    log.debug("Accepted connection: " + ses.remoteAddress());
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                if (e != null)
                    log.error("Connection failed with exception", e);
            }

            @Override public void onMessage(GridNioSession ses, byte[] msg) {
                addMessage(converter.convert(msg));
            }
        };

        ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

        GridNioParser parser = F.isEmpty(delim) ? new GridBufferedParser(directMode, byteOrder) :
            new GridDelimitedParser(delim, directMode);

        if (converter == null)
            converter = new DefaultConverter<>(getIgnite().name());

        GridNioFilter codec = new GridNioCodecFilter(parser, log, directMode);

        GridNioFilter[] filters = new GridNioFilter[] {codec};

        try {
            srv = new GridNioServer.Builder<byte[]>()
                .address(addr == null ? InetAddress.getLocalHost() : addr)
                .serverName("sock-streamer")
                .port(port)
                .listener(lsnr)
                .logger(log)
                .selectorCount(threads)
                .byteOrder(byteOrder)
                .filters(filters)
                .build();
        }
        catch (IgniteCheckedException | UnknownHostException e) {
            throw new IgniteException(e);
        }

        srv.start();

        if (log.isDebugEnabled())
            log.debug("Socket streaming server started on " + addr + ':' + port);
    }

    /**
     * Stops streamer.
     */
    public void stop() {
        if (srv != null)
            srv.stop();

        if (log.isDebugEnabled())
            log.debug("Socket streaming server stopped");
    }

    /**
     * Converts message to Java object using Jdk marshaller.
     */
    private static class DefaultConverter<T> implements SocketMessageConverter<T> {
        /** Marshaller. */
        private final Marshaller marsh;

        /**
         * Constructor.
         *
         * @param gridName Grid name.
         */
        private DefaultConverter(@Nullable String gridName) {
            marsh = MarshallerUtils.jdkMarshaller(gridName);
        }

        /** {@inheritDoc} */
        @Override public T convert(byte[] msg) {
            try {
                return U.unmarshal(marsh, msg, null);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}