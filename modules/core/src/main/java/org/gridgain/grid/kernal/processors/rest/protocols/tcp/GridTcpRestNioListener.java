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
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.kernal.processors.rest.handlers.cache.*;
import org.gridgain.grid.kernal.processors.rest.request.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;
import java.util.*;

import static org.gridgain.grid.kernal.GridProductImpl.*;
import static org.gridgain.grid.kernal.processors.rest.GridRestCommand.*;
import static org.gridgain.grid.kernal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.*;

/**
 * Listener for nio server that handles incoming tcp rest packets.
 */
public class GridTcpRestNioListener extends GridNioServerListenerAdapter<GridClientMessage> {
    /** Protobuf marshaller class name. */
    private static final String PROTOBUF_MARSH_CLS =
        "org.gridgain.client.marshaller.protobuf.GridClientProtobufMarshaller";

    /** Logger. */
    private GridLogger log;

    /** Protocol handler. */
    private GridRestProtocolHandler hnd;

    /** Handler for all memcache requests */
    private GridTcpMemcachedNioListener memcachedLsnr;

    /** Supported marshallers. */
    @GridToStringExclude
    private final Map<Byte, GridClientMarshaller> suppMarshMap;

    /** Mapping of {@code GridCacheOperation} to {@code GridRestCommand}. */
    private static final Map<GridClientCacheRequest.GridCacheOperation, GridRestCommand> cacheCmdMap =
        new EnumMap<>(GridClientCacheRequest.GridCacheOperation.class);

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
    }

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

        tmpMap.put(GridClientJdkMarshaller.PROTOCOL_ID, new GridClientJdkMarshaller());

        addProtobufMarshaller(tmpMap);

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

    /**
     * @param map Marshallers map.
     */
    private void addProtobufMarshaller(Map<Byte, GridClientMarshaller> map) {
        try {
            Class<?> cls = Class.forName(PROTOBUF_MARSH_CLS);

            Constructor<?> cons = cls.getConstructor();

            GridClientMarshaller marsh = (GridClientMarshaller)cons.newInstance();

            Byte protoId = (Byte)cls.getField("PROTOCOL_ID").get(null);

            map.put(protoId, marsh);
        }
        catch (ClassNotFoundException ignored) {
            U.quietAndWarn(log, "Failed to create Protobuf marshaller for REST (C++ and .NET clients won't work). " +
                "Consider adding gridgain-protobuf module to classpath.");
        }
        catch (InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException |
            NoSuchFieldException e) {
            U.error(log, "Failed to create Protobuf marshaller for REST.", e);
        }
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
                GridClientHandshakeRequest hs = (GridClientHandshakeRequest)msg;

                byte[] verBytes = hs.versionBytes();

                if (!Arrays.equals(VER_BYTES, verBytes))
                    U.warn(log, "Client version check failed [ses=" + ses +
                        ", expected=" + Arrays.toString(VER_BYTES)
                        + ", actual=" + Arrays.toString(verBytes) + ']');

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

                ses.send(GridClientHandshakeResponse.OK);
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
                                U.error(log, "Failed to process client request: " + msg, e);

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

            Map vals = req.values();
            if (vals != null)
                restCacheReq.values(new HashMap<Object, Object>(vals));

            restCacheReq.command(cacheCmdMap.get(req.operation()));

            restReq = restCacheReq;
        }
        else if (msg instanceof GridClientTaskRequest) {
            GridClientTaskRequest req = (GridClientTaskRequest) msg;

            GridRestTaskRequest restTaskReq = new GridRestTaskRequest();

            restTaskReq.command(EXE);

            restTaskReq.taskName(req.taskName());
            restTaskReq.params(Arrays.asList(req.argument()));

            restReq = restTaskReq;
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
