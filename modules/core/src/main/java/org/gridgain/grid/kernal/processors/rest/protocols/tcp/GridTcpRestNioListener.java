/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.protocols.tcp;

import org.gridgain.client.marshaller.*;
import org.gridgain.client.marshaller.jdk.*;
import org.gridgain.client.marshaller.optimized.*;
import org.gridgain.client.marshaller.protobuf.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.kernal.processors.rest.handlers.cache.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.kernal.GridProductImpl.*;
import static org.gridgain.grid.kernal.processors.rest.GridRestCommand.*;

/**
 * Listener for nio server that handles incoming tcp rest packets.
 */
public class GridTcpRestNioListener extends GridNioServerListenerAdapter<GridClientMessage> {
    /** Logger. */
    private GridLogger log;

    /** Protocol handler. */
    private GridRestProtocolHandler hnd;

    /** Handler for all memcache requests */
    private GridTcpMemcachedNioListener memcachedLsnr;

    /** Supported marshallers. */
    @GridToStringExclude
    private final Map<Byte, GridClientMarshaller> suppMarshMap;

    /**
     * Creates listener which will convert incoming tcp packets to rest requests and forward them to
     * a given rest handler.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     */
    public GridTcpRestNioListener(GridLogger log, GridRestProtocolHandler hnd) {
        memcachedLsnr = new GridTcpMemcachedNioListener(log, hnd);

        this.log = log;
        this.hnd = hnd;

        Map<Byte, GridClientMarshaller> tmpMap = new GridLeanMap<>(3);

        tmpMap.put(GridClientProtobufMarshaller.PROTOCOL_ID, new GridClientProtobufMarshaller());
        tmpMap.put(GridClientJdkMarshaller.PROTOCOL_ID, new GridClientJdkMarshaller());

        // Special case for Optimized marshaller, which may throw exception.
        // This may happen, for example, if some Unsafe methods are unavailable.
        try {
            tmpMap.put(GridClientOptimizedMarshaller.PROTOCOL_ID, new GridClientOptimizedMarshaller());
        }
        catch (Exception e) {
            U.warn(
                log,
                "Failed to create " + GridClientOptimizedMarshaller.class.getSimpleName() +
                    " for handling client communication (" + e.getMessage() +
                    "). Local node will operate without this marshaller.",
                "Failed to create " + GridClientOptimizedMarshaller.class.getSimpleName() + '.');
        }

        suppMarshMap = Collections.unmodifiableMap(tmpMap);
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
                ses.send(GridClientPingPacket.PING_MESSAGE);
            else if (msg instanceof GridClientHandshakeRequest) {
                GridClientHandshakeResponse res = null;

                GridClientHandshakeRequest hs = (GridClientHandshakeRequest)msg;

                byte[] verBytes = hs.versionBytes();

                if (!Arrays.equals(VER_BYTES, verBytes)) {
                    log.warning("Client version check failed [ses=" + ses +
                        ", expected=" + Arrays.toString(VER_BYTES)
                        + ", actual=" + Arrays.toString(verBytes) + ']');

                    res = GridClientHandshakeResponse.ERR_VERSION_CHECK_FAILED;
                }

                GridClientMarshaller marsh = suppMarshMap.get(hs.protocolId());

                if (marsh == null) {
                    log.error("No marshaller found with given protocol ID [protocolId=" + hs.protocolId() + ']');

                    ses.send(GridClientHandshakeResponse.ERR_UNKNOWN_PROTO_ID).listenAsync(
                        new CI1<GridNioFuture<?>>() {
                            @Override public void apply(GridNioFuture<?> fut) {
                                ses.close();
                            }
                        });

                    return;
                }

                ses.addMeta(GridNioSessionMetaKey.MARSHALLER.ordinal(), marsh);

                ses.send(res == null ? GridClientHandshakeResponse.OK : res);
            }
            else {
                final GridRestRequest req = createRestRequest(msg);

                if (req != null)
                    hnd.handleAsync(req).listenAsync(new CI1<GridFuture<GridRestResponse>>() {
                        @Override public void apply(GridFuture<GridRestResponse> fut) {
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
                            catch (GridException e) {
                                U.error(log, "Failed to process client request: " + req, e);

                                res.successStatus(GridClientResponse.STATUS_FAILED);
                                res.errorMessage("Failed to process client request: " + e.getMessage());
                            }

                            ses.send(res);
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
     * @param msg Request message.
     * @return REST request object.
     */
    @Nullable private GridRestRequest createRestRequest(GridClientMessage msg) {
        GridRestRequest restReq = null;

        if (msg instanceof GridClientAuthenticationRequest) {
            GridClientAuthenticationRequest req = (GridClientAuthenticationRequest)msg;

            restReq = new GridRestRequest();

            restReq.setCommand(NOOP);

            restReq.setCredentials(req.credentials());
        }
        else if (msg instanceof GridClientCacheRequest) {
            GridClientCacheRequest req = (GridClientCacheRequest)msg;

            Map<?, ?> vals = req.values();

            restReq = new GridRestRequest();

            Map<String, Object> params = new GridLeanMap<>(4);

            params.put("cacheName", req.cacheName());

            int i = 1;

            switch (req.operation()) {
                case PUT:
                    restReq.setCommand(CACHE_PUT);

                    params.put("key", req.key());
                    params.put("val", req.value());

                    break;
                case PUT_ALL:
                    restReq.setCommand(CACHE_PUT_ALL);

                    for (Map.Entry entry : vals.entrySet()) {
                        params.put("k" + i, entry.getKey());
                        params.put("v" + i, entry.getValue());

                        i++;
                    }

                    break;
                case GET:
                    restReq.setCommand(CACHE_GET);

                    params.put("key", req.key());

                    break;
                case GET_ALL:
                    restReq.setCommand(CACHE_GET_ALL);

                    for (Map.Entry entry : vals.entrySet()) {
                        params.put("k" + i, entry.getKey());

                        i++;
                    }

                    break;
                case RMV:
                    restReq.setCommand(CACHE_REMOVE);

                    params.put("key", req.key());

                    break;
                case RMV_ALL:
                    restReq.setCommand(CACHE_REMOVE_ALL);

                    for (Map.Entry entry : vals.entrySet()) {
                        params.put("k" + i, entry.getKey());

                        i++;
                    }

                    break;
                case REPLACE:
                    restReq.setCommand(CACHE_REPLACE);

                    params.put("key", req.key());
                    params.put("val", req.value());

                    break;
                case CAS:
                    restReq.setCommand(CACHE_CAS);

                    params.put("key", req.key());
                    params.put("val1", req.value());
                    params.put("val2", req.value2());

                    break;
                case METRICS:
                    restReq.setCommand(CACHE_METRICS);

                    params.put("key", req.key());

                    break;
                case APPEND:
                    restReq.setCommand(CACHE_APPEND);

                    params.put("key", req.key());
                    params.put("val", req.value());

                    break;
                case PREPEND:
                    restReq.setCommand(CACHE_PREPEND);

                    params.put("key", req.key());
                    params.put("val", req.value());

                    break;
            }

            if (req.cacheFlagsOn() != 0)
                params.put("cacheFlags", Integer.toString(req.cacheFlagsOn()));

            restReq.setParameters(params);
        }
        else if (msg instanceof GridClientTaskRequest) {
            GridClientTaskRequest req = (GridClientTaskRequest)msg;

            restReq = new GridRestRequest();

            restReq.setCommand(EXE);

            Map<String, Object> params = new GridLeanMap<>(1);

            params.put("name", req.taskName());

            params.put("p1", req.argument());

            restReq.setParameters(params);
        }
        else if (msg instanceof GridClientTopologyRequest) {
            GridClientTopologyRequest req = (GridClientTopologyRequest)msg;

            restReq = new GridRestRequest();

            restReq.setCommand(TOPOLOGY);

            Map<String, Object> params = new GridLeanMap<>(2);

            params.put("mtr", req.includeMetrics());
            params.put("attr", req.includeAttributes());

            if (req.nodeId() != null) {
                restReq.setCommand(NODE);

                params.put("id", req.nodeId().toString());
            }
            else if (req.nodeIp() != null) {
                restReq.setCommand(NODE);

                params.put("ip", req.nodeIp());
            }
            else
                restReq.setCommand(TOPOLOGY);

            restReq.setParameters(params);
        }
        else if (msg instanceof GridClientLogRequest) {
            GridClientLogRequest req = (GridClientLogRequest)msg;

            restReq = new GridRestRequest();

            restReq.setCommand(LOG);

            Map<String, Object> params = new GridLeanMap<>(3);

            params.put("path", req.path());
            params.put("from", req.from());
            params.put("to", req.to());

            restReq.setParameters(params);
        }

        if (restReq != null) {
            restReq.setDestId(msg.destinationId());
            restReq.setClientId(msg.clientId());
            restReq.setSessionToken(msg.sessionToken());
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
