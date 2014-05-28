/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router.impl;

import org.apache.commons.lang.*;
import org.gridgain.client.*;
import org.gridgain.client.marshaller.*;
import org.gridgain.client.marshaller.jdk.*;
import org.gridgain.client.marshaller.optimized.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;
import java.util.*;

import static org.gridgain.grid.kernal.GridProductImpl.*;
import static org.gridgain.grid.util.nio.GridNioSessionMetaKey.*;

/**
 * Nio listener for the router. Extracts necessary meta information from messages
 * and delegates their delivery to underlying client.
 */
class GridTcpRouterNioListener implements GridNioServerListener<GridClientMessage> {
    /** Protobuf marshaller class name. */
    private static final String PROTOBUF_MARSH_CLS =
        "org.gridgain.client.marshaller.protobuf.GridClientProtobufMarshaller";

    /** Logger. */
    private final GridLogger log;

    /** Client for grid access. */
    private final GridRouterClientImpl client;

    /** Supported marshallers. */
    @GridToStringExclude
    private final Map<Byte, GridClientMarshaller> suppMarshMap;

    /**
     * @param log Logger.
     * @param client Client for grid access.
     */
    GridTcpRouterNioListener(GridLogger log, GridRouterClientImpl client) {
        this.log = log;
        this.client = client;

        Map<Byte, GridClientMarshaller> tmpMap = new GridLeanMap<>(3);

        tmpMap.put(GridClientOptimizedMarshaller.PROTOCOL_ID, new GridClientOptimizedMarshaller());
        tmpMap.put(GridClientJdkMarshaller.PROTOCOL_ID, new GridClientJdkMarshaller());

        addProtobufMarshaller(tmpMap);

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

            map.put(marsh.getProtocolId(), marsh);
        }
        catch (ClassNotFoundException ignored) {
            U.quietAndWarn(log, "Failed to create Protobuf marshaller for router (C++ and .NET clients won't work). " +
                "Consider adding gridgain-protobuf module to classpath.");
        }
        catch (InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
            U.error(log, "Failed to create Protobuf marshaller for router.", e);
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
                U.warn(log, "Closed client session due to exception [ses=" + ses + ", err=" + e.getMessage() + ']');
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("TypeMayBeWeakened")
    @Override public void onMessage(final GridNioSession ses, final GridClientMessage msg) {
        if (msg instanceof GridRouterRequest) {
            GridRouterRequest routerMsg = (GridRouterRequest)msg;

            final UUID clientId = routerMsg.clientId();
            final long reqId = routerMsg.requestId();

            try {
                GridClientMarshaller marsh = ses.meta(MARSHALLER.ordinal());

                assert marsh != null;

                client.forwardMessage(routerMsg, routerMsg.destinationId(), marsh.getProtocolId())
                    .listenAsync(new GridClientFutureListener() {
                        @Override public void onDone(GridClientFuture fut) {
                            try {
                                GridRouterResponse res = (GridRouterResponse)fut.get();
                                // Restoring original request id, because it was overwritten by the client.
                                res.requestId(reqId);

                                ses.send(res);
                            }
                            catch (GridClientException e) {
                                ses.send(makeFailureResponse(e, clientId, reqId));
                            }
                        }
                    });
            }
            catch (GridClientException e) {
                ses.send(makeFailureResponse(e, clientId, reqId));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                U.warn(
                    log,
                    "Message forwarding was interrupted (will ignore last message): " + e.getMessage(),
                    "Message forwarding was interrupted.");
            }
        }
        else if (msg instanceof GridClientHandshakeRequest) {
            GridClientHandshakeRequest hs = (GridClientHandshakeRequest)msg;

            byte[] verBytes = hs.versionBytes();

            if (!Arrays.equals(VER_BYTES, verBytes))
                U.warn(log, "Client version check failed [ses=" + ses +
                    ", expected=" + Arrays.toString(VER_BYTES)
                    + ", actual=" + Arrays.toString(verBytes) + ']');

            final byte protoId = hs.protocolId();

            GridClientMarshaller marsh = suppMarshMap.get(protoId);

            if (marsh == null) {
                U.warn(log,
                    "No marshaller found for a given protocol ID (will use a stub): " + protoId);

                // Use a marshaller stub to just save protocol ID.
                ses.addMeta(MARSHALLER.ordinal(), new GridClientMarshaller() {
                    @Override public byte[] marshal(Object obj) {
                        U.warn(log, "Attempt to marshal a message with a stub " +
                            "(will output empty result): " + obj);

                        return ArrayUtils.EMPTY_BYTE_ARRAY;
                    }

                    @Override public <T> T unmarshal(byte[] bytes) {
                        assert false : "Attempt to unmarshal a message with a stub.";

                        return null;
                    }

                    @Override public byte getProtocolId() {
                        return protoId;
                    }
                });
            }
            else
                ses.addMeta(MARSHALLER.ordinal(), marsh);

            ses.send(GridClientHandshakeResponse.OK);
        }
        else if (msg instanceof GridClientPingPacket)
            ses.send(GridClientPingPacket.PING_MESSAGE);
        else
            throw new IllegalArgumentException("Unsupported input message: " + msg);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) {
        U.warn(log, "Closing NIO session because of write timeout.");

        ses.close();
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) {
        U.warn(log, "Closing NIO session because of idle.");

        ses.close();
    }

    /**
     * Creates a failure response, based on the given exception.
     *
     * @param e Exception to extract failure report from.
     * @param clientId Client id.
     * @param reqId Request id.
     * @return Failure response.
     */
    private GridClientResponse makeFailureResponse(GridClientException e, UUID clientId, Long reqId) {
        U.error(log, "Failed to process message on router.", e);

        GridClientResponse res = new GridClientResponse();

        res.clientId(clientId);
        res.requestId(reqId);
        res.successStatus(GridClientResponse.STATUS_FAILED);
        res.errorMessage("Failed to process message on router " +
            "[exception=" + e.getClass().getSimpleName() + ", message=" + e.getMessage() + ']');

        return res;
    }
}
