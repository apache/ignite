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

package org.apache.ignite.internal.processors.rest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.marshaller.GridClientMarshaller;
import org.apache.ignite.internal.client.marshaller.optimized.GridClientOptimizedMarshaller;
import org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest;
import org.apache.ignite.internal.processors.rest.client.message.GridClientHandshakeRequest;
import org.apache.ignite.internal.processors.rest.client.message.GridClientHandshakeResponse;
import org.apache.ignite.internal.processors.rest.client.message.GridClientMessage;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeBean;
import org.apache.ignite.internal.processors.rest.client.message.GridClientResponse;
import org.apache.ignite.internal.processors.rest.client.message.GridClientTaskRequest;
import org.apache.ignite.internal.processors.rest.client.message.GridClientTaskResultBean;
import org.apache.ignite.internal.processors.rest.client.message.GridClientTopologyRequest;
import org.apache.ignite.internal.processors.rest.protocols.tcp.GridMemcachedMessage;
import org.apache.ignite.internal.util.GridClientByteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.java.JavaLogger;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.APPEND;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.CAS;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.GET;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.GET_ALL;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.METRICS;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.PREPEND;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.PUT_ALL;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.REPLACE;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.RMV;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.RMV_ALL;

/**
 * Test client.
 */
final class TestBinaryClient {
    /** Logger. */
    private final IgniteLogger log = new JavaLogger();

    /** Marshaller. */
    private final GridClientMarshaller marsh = new GridClientOptimizedMarshaller();

    /** Socket. */
    private final Socket sock;

    /** Socket input stream. */
    private final InputStream input;

    /** Opaque counter. */
    private final AtomicInteger idCntr = new AtomicInteger(0);

    /** Response queue. */
    private final BlockingQueue<Response> queue = new LinkedBlockingQueue<>();

    /** Socket reader. */
    private final Thread rdr;

    /** Quit response. */
    private static final Response QUIT_RESP = new Response(0, GridRestResponse.STATUS_FAILED, null, null);

    /** Random client id. */
    private UUID id = UUID.randomUUID();

    /**
     * Creates client.
     *
     * @param host Hostname.
     * @param port Port number.
     * @throws IgniteCheckedException In case of error.
     */
    TestBinaryClient(String host, int port) throws IgniteCheckedException {
        assert host != null;
        assert port > 0;

        try {
            sock = new Socket(host, port);

            input = sock.getInputStream();

            GridClientHandshakeRequest req = new GridClientHandshakeRequest();

            req.marshallerId(GridClientOptimizedMarshaller.ID);

            // Write handshake.
            sock.getOutputStream().write(GridMemcachedMessage.IGNITE_HANDSHAKE_FLAG);
            sock.getOutputStream().write(req.rawBytes());

            byte[] buf = new byte[2];

            // Wait for handshake response.
            int read = input.read(buf);

            assert read == 2 : read;

            assert buf[0] == GridMemcachedMessage.IGNITE_HANDSHAKE_RES_FLAG;
            assert buf[1] == GridClientHandshakeResponse.OK.resultCode() :
                "Client handshake failed [code=" + buf[0] + ']';
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to establish connection.", e);
        }

        // Start socket reader thread.
        rdr = new Thread(new Runnable() {
            @SuppressWarnings("InfiniteLoopStatement")
            @Override public void run() {
                try {
                    ByteArrayOutputStream buf = new ByteArrayOutputStream();

                    int len = 0;

                    boolean running = true;

                    while (running) {
                        // Header.
                        int symbol = input.read();

                        if (symbol == -1)
                            break;

                        if ((byte)symbol != (byte)0x90) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to parse incoming packet (invalid packet start): " +
                                    Integer.toHexString(symbol & 0xFF));

                            break;
                        }

                        // Packet.
                        while (true) {
                            symbol = input.read();

                            if (symbol == -1) {
                                running = false;

                                break;
                            }

                            byte b = (byte)symbol;

                            buf.write(b);

                            if (len == 0) {
                                if (buf.size() == 4) {
                                    len = U.bytesToInt(buf.toByteArray(), 0);

                                    if (log.isInfoEnabled())
                                        log.info("Read length: " + len);

                                    buf.reset();
                                }
                            }
                            else {
                                if (buf.size() == len) {
                                    byte[] bytes = buf.toByteArray();
                                    byte[] hdrBytes = Arrays.copyOfRange(bytes, 0, 40);
                                    byte[] msgBytes = Arrays.copyOfRange(bytes, 40, bytes.length);

                                    GridClientResponse msg = marsh.unmarshal(msgBytes);

                                    long reqId = GridClientByteUtils.bytesToLong(hdrBytes, 0);
                                    UUID clientId = GridClientByteUtils.bytesToUuid(hdrBytes, 8);
                                    UUID destId = GridClientByteUtils.bytesToUuid(hdrBytes, 24);

                                    msg.requestId(reqId);
                                    msg.clientId(clientId);
                                    msg.destinationId(destId);

                                    buf.reset();

                                    len = 0;

                                    queue.offer(new Response(msg.requestId(), msg.successStatus(), msg.result(),
                                        msg.errorMessage()));

                                    break;
                                }
                            }
                        }
                    }
                }
                catch (IOException e) {
                    if (!Thread.currentThread().isInterrupted())
                        U.error(log, e);
                }
                finally {
                    U.closeQuiet(sock);

                    queue.add(QUIT_RESP);
                }
            }
        });

        rdr.start();
    }

    /** {@inheritDoc} */
    public void shutdown() throws IgniteCheckedException {
        try {
            if (rdr != null) {
                rdr.interrupt();

                U.closeQuiet(sock);

                rdr.join();
            }
        }
        catch (InterruptedException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Makes request to server and waits for response.
     *
     * @param msg Message to request,
     * @return Response object.
     * @throws IgniteCheckedException If request failed.
     */
    private Response makeRequest(GridClientMessage msg) throws IgniteCheckedException {
        assert msg != null;

        // Send request
        try {
            sock.getOutputStream().write(createPacket(msg));
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to send packet.", e);
        }

        // Wait for response.
        while (true) {
            try {
                // Take response from queue.
                Response res = queue.take();

                if (res == QUIT_RESP)
                    return res;

                // Check opaque value.
                if (res.opaque() == msg.requestId()) {
                    if (!res.isSuccess() && res.error() != null)
                        throw new IgniteCheckedException(res.error());
                    else
                        return res;
                }
                else
                    // Return response to queue if opaque is incorrect.
                    queue.add(res);
            }
            catch (InterruptedException e) {
                throw new IgniteCheckedException("Interrupted while waiting for response.", e);
            }
        }

    }

    /**
     * Creates hessian packet from client message.
     *
     * @param msg Message to be sent.
     * @return Raw packet.
     * @throws IOException If serialization failed.
     */
    private byte[] createPacket(GridClientMessage msg) throws IOException {
        msg.clientId(id);

        ByteBuffer res = marsh.marshal(msg, 45);

        ByteBuffer slice = res.slice();

        slice.put((byte)0x90);
        slice.putInt(res.remaining() - 5);
        slice.putLong(msg.requestId());
        slice.put(U.uuidToBytes(msg.clientId()));
        slice.put(U.uuidToBytes(msg.destinationId()));

        byte[] arr = new byte[res.remaining()];

        res.get(arr);

        return arr;
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @return If value was actually put.
     * @throws IgniteCheckedException In case of error.
     */
    public <K, V> boolean cachePut(@Nullable String cacheName, K key, V val)
        throws IgniteCheckedException {
        return cachePutAll(cacheName, Collections.singletonMap(key, val));
    }

    /**
     * @param cacheName Cache name.
     * @param entries Entries.
     * @return {@code True} if map contained more then one entry or if put succeeded in case of one entry,
     *      {@code false} otherwise
     * @throws IgniteCheckedException In case of error.
     */
    public <K, V> boolean cachePutAll(@Nullable String cacheName, Map<K, V> entries)
        throws IgniteCheckedException {
        assert entries != null;

        GridClientCacheRequest req = new GridClientCacheRequest(PUT_ALL);

        req.requestId(idCntr.incrementAndGet());
        req.cacheName(cacheName);
        req.values((Map<Object, Object>)entries);

        return makeRequest(req).<Boolean>getObject();
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @return Value.
     * @throws IgniteCheckedException In case of error.
     */
    public <K, V> V cacheGet(@Nullable String cacheName, K key)
        throws IgniteCheckedException {
        assert key != null;

        GridClientCacheRequest req = new GridClientCacheRequest(GET);

        req.requestId(idCntr.getAndIncrement());
        req.cacheName(cacheName);
        req.key(key);

        return makeRequest(req).getObject();

    }

    /**
     * @param cacheName Cache name.
     * @param keys Keys.
     * @return Entries.
     * @throws IgniteCheckedException In case of error.
     */
    public <K, V> Map<K, V> cacheGetAll(@Nullable String cacheName, K... keys)
        throws IgniteCheckedException {
        assert keys != null;

        GridClientCacheRequest req = new GridClientCacheRequest(GET_ALL);

        req.requestId(idCntr.getAndIncrement());
        req.cacheName(cacheName);
        req.keys((Iterable<Object>)Arrays.asList(keys));

        return makeRequest(req).getObject();
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @return Whether entry was actually removed.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings("unchecked")
    public <K> boolean cacheRemove(@Nullable String cacheName, K key) throws IgniteCheckedException {
        assert key != null;

        GridClientCacheRequest req = new GridClientCacheRequest(RMV);

        req.requestId(idCntr.getAndIncrement());
        req.cacheName(cacheName);
        req.key(key);

        return makeRequest(req).<Boolean>getObject();
    }

    /**
     * @param cacheName Cache name.
     * @param keys Keys.
     * @return Whether entries were actually removed
     * @throws IgniteCheckedException In case of error.
     */
    public <K> boolean cacheRemoveAll(@Nullable String cacheName, K... keys)
        throws IgniteCheckedException {
        assert keys != null;

        GridClientCacheRequest req = new GridClientCacheRequest(RMV_ALL);

        req.requestId(idCntr.getAndIncrement());
        req.cacheName(cacheName);
        req.keys((Iterable<Object>)Arrays.asList(keys));

        return makeRequest(req).isSuccess();
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @return Whether value was actually replaced.
     * @throws IgniteCheckedException In case of error.
     */
    public <K, V> boolean cacheReplace(@Nullable String cacheName, K key, V val)
        throws IgniteCheckedException {
        assert key != null;
        assert val != null;

        GridClientCacheRequest replace = new GridClientCacheRequest(REPLACE);

        replace.requestId(idCntr.getAndIncrement());
        replace.cacheName(cacheName);
        replace.key(key);
        replace.value(val);

        return makeRequest(replace).<Boolean>getObject();
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val1 Value 1.
     * @param val2 Value 2.
     * @return Whether new value was actually set.
     * @throws IgniteCheckedException In case of error.
     */
    public <K, V> boolean cacheCompareAndSet(@Nullable String cacheName, K key, @Nullable V val1, @Nullable V val2)
        throws IgniteCheckedException {
        assert key != null;

        GridClientCacheRequest msg = new GridClientCacheRequest(CAS);

        msg.requestId(idCntr.getAndIncrement());
        msg.cacheName(cacheName);
        msg.key(key);
        msg.value(val1);
        msg.value2(val2);

        return makeRequest(msg).<Boolean>getObject();
    }

    /**
     * @param cacheName Cache name.
     * @return Metrics.
     * @throws IgniteCheckedException In case of error.
     */
    public <K> Map<String, Long> cacheMetrics(@Nullable String cacheName) throws IgniteCheckedException {
        GridClientCacheRequest metrics = new GridClientCacheRequest(METRICS);

        metrics.requestId(idCntr.getAndIncrement());
        metrics.cacheName(cacheName);

        return makeRequest(metrics).getObject();
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @return Whether entry was appended.
     * @throws IgniteCheckedException In case of error.
     */
    public <K, V> boolean cacheAppend(@Nullable String cacheName, K key, V val)
        throws IgniteCheckedException {
        assert key != null;
        assert val != null;

        GridClientCacheRequest add = new GridClientCacheRequest(APPEND);

        add.requestId(idCntr.getAndIncrement());
        add.cacheName(cacheName);
        add.key(key);
        add.value(val);

        return makeRequest(add).<Boolean>getObject();
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @return Whether entry was prepended.
     * @throws IgniteCheckedException In case of error.
     */
    public <K, V> boolean cachePrepend(@Nullable String cacheName, K key, V val)
        throws IgniteCheckedException {
        assert key != null;
        assert val != null;

        GridClientCacheRequest add = new GridClientCacheRequest(PREPEND);

        add.requestId(idCntr.getAndIncrement());
        add.cacheName(cacheName);
        add.key(key);
        add.value(val);

        return makeRequest(add).<Boolean>getObject();
    }

    /**
     * @param taskName Task name.
     * @param arg Task arguments.
     * @return Task execution result.
     * @throws IgniteCheckedException In case of error.
     */
    public GridClientTaskResultBean execute(String taskName, @Nullable Object arg) throws IgniteCheckedException {
        assert !F.isEmpty(taskName);

        GridClientTaskRequest msg = new GridClientTaskRequest();

        msg.taskName(taskName);
        msg.argument(arg);

        return makeRequest(msg).getObject();
    }

    /**
     * @param id Node ID.
     * @param includeAttrs Whether to include attributes.
     * @param includeMetrics Whether to include metrics.
     * @return Node.
     * @throws IgniteCheckedException In case of error.
     */
    public GridClientNodeBean node(UUID id, boolean includeAttrs, boolean includeMetrics)
        throws IgniteCheckedException {
        assert id != null;

        GridClientTopologyRequest msg = new GridClientTopologyRequest();

        msg.nodeId(id);
        msg.includeAttributes(includeAttrs);
        msg.includeMetrics(includeMetrics);

        return makeRequest(msg).getObject();
    }

    /**
     * @param ipAddr IP address.
     * @param includeAttrs Whether to include attributes.
     * @param includeMetrics Whether to include metrics.
     * @return Node.
     * @throws IgniteCheckedException In case of error.
     */
    public GridClientNodeBean node(String ipAddr, boolean includeAttrs, boolean includeMetrics)
        throws IgniteCheckedException {
        assert !F.isEmpty(ipAddr);

        GridClientTopologyRequest msg = new GridClientTopologyRequest();

        msg.nodeIp(ipAddr);
        msg.includeAttributes(includeAttrs);
        msg.includeMetrics(includeMetrics);

        return makeRequest(msg).getObject();
    }

    /**
     * @param includeAttrs Whether to include attributes.
     * @param includeMetrics Whether to include metrics.
     * @return Nodes.
     * @throws IgniteCheckedException In case of error.
     */
    public List<GridClientNodeBean> topology(boolean includeAttrs, boolean includeMetrics)
        throws IgniteCheckedException {
        GridClientTopologyRequest msg = new GridClientTopologyRequest();

        msg.includeAttributes(includeAttrs);
        msg.includeMetrics(includeMetrics);

        return makeRequest(msg).getObject();
    }

    /**
     * Response data.
     */
    private static class Response {
        /** Opaque. */
        private final long opaque;

        /** Success flag. */
        private final int success;

        /** Response object. */
        private final Object obj;

        /** Error message, if any */
        private final String error;

        /**
         * @param opaque Opaque.
         * @param success Success flag.
         * @param obj Response object.
         * @param error Error message, if any.
         */
        Response(long opaque, int success, @Nullable Object obj, @Nullable String error) {
            assert opaque >= 0;

            this.opaque = opaque;
            this.success = success;
            this.obj = obj;
            this.error = error;
        }

        /**
         * @return Opaque.
         */
        long opaque() {
            return opaque;
        }

        /**
         * @return Success flag.
         */
        boolean isSuccess() {
            return success == GridRestResponse.STATUS_SUCCESS;
        }

        /**
         * @return Response object.
         */
        @SuppressWarnings("unchecked")
        <T> T getObject() {
            return (T)obj;
        }

        /**
         * @return Error message in case if error occurred.
         */
        String error() {
            return error;
        }
    }
}