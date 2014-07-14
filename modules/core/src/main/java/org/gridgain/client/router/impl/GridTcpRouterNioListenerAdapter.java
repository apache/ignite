/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router.impl;

import org.gridgain.client.*;
import org.gridgain.client.marshaller.*;
import org.gridgain.client.marshaller.jdk.*;
import org.gridgain.client.marshaller.optimized.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.util.nio.GridNioSessionMetaKey.*;

/**
 * Nio listener for the router. Extracts necessary meta information from messages
 * and delegates their delivery to underlying client.
 */
abstract class GridTcpRouterNioListenerAdapter implements GridNioServerListener<GridClientMessage> {
    /** Supported protocol versions. */
    private static final Collection<Short> SUPP_VERS = new HashSet<>();

    /**
     */
    static {
        SUPP_VERS.add((short)1);
    }

    /** Logger. */
    private final GridLogger log;

    /** Client for grid access. */
    private final GridRouterClientImpl client;

    /** Marshallers map. */
    protected final Map<Byte, GridClientMarshaller> marshMap;

    /**
     * @param log Logger.
     * @param client Client for grid access.
     */
    @SuppressWarnings({"AbstractMethodCallInConstructor", "OverriddenMethodCallDuringObjectConstruction"})
    GridTcpRouterNioListenerAdapter(GridLogger log, GridRouterClientImpl client) {
        this.log = log;
        this.client = client;

        marshMap = new HashMap<>();

        marshMap.put(GridClientOptimizedMarshaller.ID, new GridClientOptimizedMarshaller());
        marshMap.put(GridClientJdkMarshaller.ID, new GridClientJdkMarshaller());

        init();
    }

    /**
     */
    protected abstract void init();

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
                client.forwardMessage(routerMsg, routerMsg.destinationId(), ses.<Byte>meta(MARSHALLER_ID.ordinal()))
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

            short ver = hs.version();

            if (!SUPP_VERS.contains(ver)) {
                U.error(log, "Client protocol version is not supported [ses=" + ses +
                    ", ver=" + ver +
                    ", supported=" + SUPP_VERS + ']');

                ses.close();
            }
            else {
                byte marshId = hs.marshallerId();

                GridClientMarshaller marsh = marshMap.get(marshId);

                if (marsh == null) {
                    U.error(log, "Client marshaller ID is invalid. Note that .NET and C++ clients " +
                        "are supported only in enterprise edition [ses=" + ses + ", marshId=" + marshId + ']');

                    ses.close();
                }
                else {
                    ses.addMeta(MARSHALLER_ID.ordinal(), marshId);
                    ses.addMeta(MARSHALLER.ordinal(), marsh);

                    ses.send(GridClientHandshakeResponse.OK);
                }
            }
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
