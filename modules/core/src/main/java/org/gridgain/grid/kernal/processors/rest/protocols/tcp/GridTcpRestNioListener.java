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

package org.gridgain.grid.kernal.processors.rest.protocols.tcp;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.client.marshaller.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.kernal.processors.rest.handlers.cache.*;
import org.gridgain.grid.kernal.processors.rest.request.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.kernal.processors.rest.GridRestCommand.*;
import static org.gridgain.grid.kernal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.*;
import static org.gridgain.grid.kernal.processors.rest.client.message.GridClientHandshakeResponse.*;
import static org.gridgain.grid.util.nio.GridNioSessionMetaKey.*;

/**
 * Listener for nio server that handles incoming tcp rest packets.
 */
public class GridTcpRestNioListener extends GridNioServerListenerAdapter<GridClientMessage> {
    /** Mapping of {@code GridCacheOperation} to {@code GridRestCommand}. */
    private static final Map<GridClientCacheRequest.GridCacheOperation, GridRestCommand> cacheCmdMap =
        new EnumMap<>(GridClientCacheRequest.GridCacheOperation.class);

    /** Supported protocol versions. */
    private static final Collection<Short> SUPP_VERS = new HashSet<>();

    /**
     * Fills {@code cacheCmdMap}.
     */
    static {
        cacheCmdMap.put(PUT, CACHE_PUT);
        cacheCmdMap.put(PUT_ALL, CACHE_PUT_ALL);
        cacheCmdMap.put(GET, CACHE_GET);
        cacheCmdMap.put(GET_ALL, CACHE_GET_ALL);
        cacheCmdMap.put(RMV, CACHE_REMOVE);
        cacheCmdMap.put(RMV_ALL, CACHE_REMOVE_ALL);
        cacheCmdMap.put(REPLACE, CACHE_REPLACE);
        cacheCmdMap.put(CAS, CACHE_CAS);
        cacheCmdMap.put(METRICS, CACHE_METRICS);
        cacheCmdMap.put(APPEND, CACHE_APPEND);
        cacheCmdMap.put(PREPEND, CACHE_PREPEND);

        SUPP_VERS.add((short)1);
    }

    /** */
    private final CountDownLatch marshMapLatch = new CountDownLatch(1);

    /** Marshallers map. */
    private Map<Byte, GridClientMarshaller> marshMap;

    /** Logger. */
    private IgniteLogger log;

    /** Protocol. */
    private GridTcpRestProtocol proto;

    /** Protocol handler. */
    private GridRestProtocolHandler hnd;

    /** Handler for all memcache requests */
    private GridTcpMemcachedNioListener memcachedLsnr;

    /**
     * Creates listener which will convert incoming tcp packets to rest requests and forward them to
     * a given rest handler.
     *
     * @param log Logger to use.
     * @param proto Protocol.
     * @param hnd Rest handler.
     * @param ctx Context.
     */
    public GridTcpRestNioListener(IgniteLogger log, GridTcpRestProtocol proto, GridRestProtocolHandler hnd,
        GridKernalContext ctx) {
        memcachedLsnr = new GridTcpMemcachedNioListener(log, hnd, ctx);

        this.log = log;
        this.proto = proto;
        this.hnd = hnd;
    }

    /**
     * @param marshMap Marshallers.
     */
    void marshallers(Map<Byte, GridClientMarshaller> marshMap) {
        assert marshMap != null;

        this.marshMap = marshMap;

        marshMapLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        if (e != null) {
            if (e instanceof RuntimeException)
                U.error(log, "Failed to process request from remote client: " + ses, e);
            else
                U.warn(log, "Closed client session due to exception [ses=" + ses + ", msg=" + e.getMessage() + ']');
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public void onMessage(final GridNioSession ses, final GridClientMessage msg) {
        if (msg instanceof GridMemcachedMessage)
            memcachedLsnr.onMessage(ses, (GridMemcachedMessage)msg);
        else {
            if (msg == GridClientPingPacket.PING_MESSAGE)
                ses.send(new GridClientPingPacketWrapper());
            else if (msg instanceof GridClientHandshakeRequest) {
                GridClientHandshakeRequest hs = (GridClientHandshakeRequest)msg;

                short ver = hs.version();

                if (!SUPP_VERS.contains(ver)) {
                    U.error(log, "Client protocol version is not supported [ses=" + ses +
                        ", ver=" + ver +
                        ", supported=" + SUPP_VERS + ']');

                    ses.close();
                }
                else {
                    byte marshId = hs.marshallerId();

                    if (marshMapLatch.getCount() > 0)
                        U.awaitQuiet(marshMapLatch);

                    GridClientMarshaller marsh = marshMap.get(marshId);

                    if (marsh == null) {
                        U.error(log, "Client marshaller ID is invalid. Note that .NET and C++ clients " +
                            "are supported only in enterprise edition [ses=" + ses + ", marshId=" + marshId + ']');

                        ses.close();
                    }
                    else {
                        ses.addMeta(MARSHALLER.ordinal(), marsh);

                        ses.send(new GridClientHandshakeResponseWrapper(CODE_OK));
                    }
                }
            }
            else {
                final GridRestRequest req = createRestRequest(ses, msg);

                if (req != null)
                    hnd.handleAsync(req).listenAsync(new CI1<IgniteFuture<GridRestResponse>>() {
                        @Override public void apply(IgniteFuture<GridRestResponse> fut) {
                            GridClientResponse res = new GridClientResponse();

                            res.requestId(msg.requestId());
                            res.clientId(msg.clientId());

                            try {
                                GridRestResponse restRes = fut.get();

                                res.sessionToken(restRes.sessionTokenBytes());
                                res.successStatus(restRes.getSuccessStatus());
                                res.errorMessage(restRes.getError());

                                Object o = restRes.getResponse();

                                // In case of metrics a little adjustment is needed.
                                if (o instanceof GridCacheRestMetrics)
                                    o = ((GridCacheRestMetrics)o).map();

                                res.result(o);
                            }
                            catch (IgniteCheckedException e) {
                                U.error(log, "Failed to process client request: " + msg, e);

                                res.successStatus(GridClientResponse.STATUS_FAILED);
                                res.errorMessage("Failed to process client request: " + e.getMessage());
                            }

                            GridClientMessageWrapper wrapper = new GridClientMessageWrapper();

                            wrapper.requestId(msg.requestId());
                            wrapper.clientId(msg.clientId());

                            try {
                                ByteBuffer bytes = proto.marshaller(ses).marshal(res, 0);

                                wrapper.message(bytes);

                                wrapper.messageSize(bytes.remaining() + 40);
                            }
                            catch (IOException e) {
                                U.error(log, "Failed to marshal response: " + res, e);

                                ses.close();

                                return;
                            }

                            ses.send(wrapper);
                        }
                    });
                else
                    U.error(log, "Failed to process client request (unknown packet type) [ses=" + ses +
                        ", msg=" + msg + ']');
            }
        }
    }

    /**
     * Creates a REST request object from client TCP binary packet.
     *
     * @param ses NIO session.
     * @param msg Request message.
     * @return REST request object.
     */
    @Nullable private GridRestRequest createRestRequest(GridNioSession ses, GridClientMessage msg) {
        GridRestRequest restReq = null;

        if (msg instanceof GridClientAuthenticationRequest) {
            GridClientAuthenticationRequest req = (GridClientAuthenticationRequest)msg;

            restReq = new GridRestTaskRequest();

            restReq.command(NOOP);

            restReq.credentials(req.credentials());
        }
        else if (msg instanceof GridClientCacheRequest) {
            GridClientCacheRequest req = (GridClientCacheRequest)msg;

            GridRestCacheRequest restCacheReq = new GridRestCacheRequest();

            restCacheReq.cacheName(req.cacheName());
            restCacheReq.cacheFlags(req.cacheFlagsOn());

            restCacheReq.key(req.key());
            restCacheReq.value(req.value());
            restCacheReq.value2(req.value2());
            restCacheReq.portableMode(proto.portableMode(ses));

            Map vals = req.values();
            if (vals != null)
                restCacheReq.values(new HashMap<Object, Object>(vals));

            restCacheReq.command(cacheCmdMap.get(req.operation()));

            restReq = restCacheReq;
        }
        else if (msg instanceof GridClientCacheQueryRequest) {
            GridClientCacheQueryRequest req = (GridClientCacheQueryRequest) msg;

            restReq = new GridRestCacheQueryRequest(req);

            switch (req.operation()) {
                case EXECUTE:
                    restReq.command(CACHE_QUERY_EXECUTE);

                    break;

                case FETCH:
                    restReq.command(CACHE_QUERY_FETCH);
                    break;

                case REBUILD_INDEXES:
                    restReq.command(CACHE_QUERY_REBUILD_INDEXES);

                    break;

                default:
                    throw new IllegalArgumentException("Unknown query operation: " + req.operation());
            }
        }
        else if (msg instanceof GridClientTaskRequest) {
            GridClientTaskRequest req = (GridClientTaskRequest) msg;

            GridRestTaskRequest restTaskReq = new GridRestTaskRequest();

            restTaskReq.command(EXE);

            restTaskReq.taskName(req.taskName());
            restTaskReq.params(Arrays.asList(req.argument()));
            restTaskReq.keepPortables(req.keepPortables());
            restTaskReq.portableMode(proto.portableMode(ses));

            restReq = restTaskReq;
        }
        else if (msg instanceof GridClientGetMetaDataRequest) {
            GridClientGetMetaDataRequest req = (GridClientGetMetaDataRequest)msg;

            restReq = new GridRestPortableGetMetaDataRequest(req);

            restReq.command(GET_PORTABLE_METADATA);
        }
        else if (msg instanceof GridClientPutMetaDataRequest) {
            GridClientPutMetaDataRequest req = (GridClientPutMetaDataRequest)msg;

            restReq = new GridRestPortablePutMetaDataRequest(req);

            restReq.command(PUT_PORTABLE_METADATA);
        }
        else if (msg instanceof GridClientTopologyRequest) {
            GridClientTopologyRequest req = (GridClientTopologyRequest) msg;

            GridRestTopologyRequest restTopReq = new GridRestTopologyRequest();

            restTopReq.includeMetrics(req.includeMetrics());
            restTopReq.includeAttributes(req.includeAttributes());

            if (req.nodeId() != null) {
                restTopReq.command(NODE);

                restTopReq.nodeId(req.nodeId());
            }
            else if (req.nodeIp() != null) {
                restTopReq.command(NODE);

                restTopReq.nodeIp(req.nodeIp());
            }
            else
                restTopReq.command(TOPOLOGY);

            restReq = restTopReq;
        }
        else if (msg instanceof GridClientLogRequest) {
            GridClientLogRequest req = (GridClientLogRequest) msg;

            GridRestLogRequest restLogReq = new GridRestLogRequest();

            restLogReq.command(LOG);

            restLogReq.path(req.path());
            restLogReq.from(req.from());
            restLogReq.to(req.to());

            restReq = restLogReq;
        }

        if (restReq != null) {
            restReq.destinationId(msg.destinationId());
            restReq.clientId(msg.clientId());
            restReq.sessionToken(msg.sessionToken());
            restReq.address(ses.remoteAddress());
        }

        return restReq;
    }

    /**
     * Closes the session by timeout (i.e. inactivity within the configured period of time).
     *
     * @param ses Session, that was inactive.
     */
    @Override public void onSessionIdleTimeout(GridNioSession ses) {
        ses.close();
    }
}
