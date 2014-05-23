/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.securesession.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.kernal.processors.rest.handlers.cache.*;
import org.gridgain.grid.kernal.processors.rest.handlers.log.*;
import org.gridgain.grid.kernal.processors.rest.handlers.task.*;
import org.gridgain.grid.kernal.processors.rest.handlers.top.*;
import org.gridgain.grid.kernal.processors.rest.handlers.version.*;
import org.gridgain.grid.kernal.processors.rest.protocols.http.jetty.*;
import org.gridgain.grid.kernal.processors.rest.protocols.tcp.*;
import org.gridgain.grid.kernal.processors.rest.request.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.kernal.processors.rest.GridRestResponse.*;
import static org.gridgain.grid.spi.GridSecuritySubjectType.*;

/**
 * Rest processor implementation.
 */
public class GridRestProcessor extends GridProcessorAdapter {
    /** */
    private static final byte[] EMPTY_ID = new byte[0];

    /** Protocols. */
    private final Collection<GridRestProtocol> protos = new ArrayList<>();

    /** Command handlers. */
    private final Map<GridRestCommand, GridRestCommandHandler> handlers = new EnumMap<>(GridRestCommand.class);

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    private final GridSecurityInterceptor securityInterceptor;

    /** Protocol handler. */
    private final GridRestProtocolHandler protoHnd = new GridRestProtocolHandler() {
        @Override public GridRestResponse handle(GridRestRequest req) throws GridException {
            return handleAsync(req).get();
        }

        @Override public GridFuture<GridRestResponse> handleAsync(final GridRestRequest req) {
            if (startLatch.getCount() > 0) {
                try {
                    startLatch.await();
                }
                catch (InterruptedException e) {
                    return new GridFinishedFuture<>(ctx, new GridException("Failed to handle request " +
                        "(protocol handler was interrupted when awaiting grid start).", e));
                }
            }

            if (log.isDebugEnabled())
                log.debug("Received request from client: " + req);

            final Object securitySubjCtx;

            try {
                securitySubjCtx = authenticate(req);

                checkPermissions(req, securitySubjCtx);
            }
            catch (GridException | GridRuntimeException e) {
                return new GridFinishedFuture<>(ctx, new GridRestResponse(STATUS_AUTH_FAILED,
                    e.getMessage()));
            }

            interceptRequest(req);

            GridRestCommandHandler hnd = handlers.get(req.command());

            GridFuture<GridRestResponse> res = hnd == null ? null : hnd.handleAsync(req);

            if (res == null)
                return new GridFinishedFuture<>(ctx,
                    new GridException("Failed to find registered handler for command: " + req.command()));

            return res.chain(new C1<GridFuture<GridRestResponse>, GridRestResponse>() {
                @Override public GridRestResponse apply(GridFuture<GridRestResponse> f) {
                    GridRestResponse res;

                    try {
                        res = f.get();
                    }
                    catch (Exception e) {
                        LT.error(log, e, "Failed to handle request: " + req.command());

                        if (log.isDebugEnabled())
                            log.debug("Failed to handle request [req=" + req + ", e=" + e + "]");

                        res = new GridRestResponse(STATUS_FAILED, e.getMessage());
                    }

                    assert res != null;

                    if (ctx.isEnterprise()) {
                        try {
                            res.sessionTokenBytes(updateSessionToken(req, securitySubjCtx));
                        }
                        catch (GridException e) {
                            U.warn(log, "Cannot update response session token: " + e.getMessage());
                        }
                    }

                    interceptResponse(res, req);

                    return res;
                }
            });
        }
    };

    /**
     * Checks security permissions.
     *
     * @param req Request to validate.
     * @param securitySubjCtx Security subject context.
     */
    private void checkPermissions(GridRestRequest req, Object securitySubjCtx) {
        if (securityInterceptor != null) {
            GridSecurityOperation op = null;
            String cacheName = null;

            switch (req.command()) {
                case CACHE_GET:
                case CACHE_GET_ALL:
                    op = GridSecurityOperation.READ;
                    cacheName = ((GridRestCacheRequest)req).cacheName();

                    break;

                case CACHE_PUT:
                case CACHE_ADD:
                case CACHE_PUT_ALL:
                case CACHE_REPLACE:
                case CACHE_INCREMENT:
                case CACHE_DECREMENT:
                case CACHE_CAS:
                case CACHE_APPEND:
                case CACHE_PREPEND:
                    op = GridSecurityOperation.PUT;
                    cacheName = ((GridRestCacheRequest)req).cacheName();

                    break;

                case CACHE_REMOVE:
                case CACHE_REMOVE_ALL:
                    op = GridSecurityOperation.REMOVE;
                    cacheName = ((GridRestCacheRequest)req).cacheName();

                    break;

                case EXE:
                case RESULT:
                    op = GridSecurityOperation.EXECUTE;
                    break;

                case VERSION:
                case LOG:
                case NOOP:
                case QUIT:
                case CACHE_METRICS:
                case TOPOLOGY:
                case NODE:
                    // No security check here.
                    return;
            }

            GridSecurityContext sCtx = new GridSecurityContextImpl(Collections.singletonList(op),
                cacheName, securitySubjCtx);

            securityInterceptor.authorize(sCtx);
        }
    }

    /**
     * Applies {@link GridClientMessageInterceptor} from {@link GridConfiguration#getClientMessageInterceptor()}
     * to all user parameters in the request.
     *
     * @param req Client request.
     */
    private void interceptRequest(GridRestRequest req) {
        GridClientMessageInterceptor interceptor = ctx.config().getClientMessageInterceptor();

        if (interceptor == null)
            return;

        if (req instanceof GridRestCacheRequest) {
            GridRestCacheRequest req0 = (GridRestCacheRequest) req;

            req0.key(interceptor.onReceive(req0.key()));
            req0.value(interceptor.onReceive(req0.value()));
            req0.value2(interceptor.onReceive(req0.value2()));

            Map<Object, Object> oldVals = req0.values();

            if (oldVals != null) {
                Map<Object, Object> newVals = new HashMap<>(oldVals.size());

                for (Map.Entry<Object, Object> e : oldVals.entrySet())
                    newVals.put(interceptor.onReceive(e.getKey()), interceptor.onReceive(e.getValue()));

                req0.values(U.sealMap(newVals));
            }
        }
        else if (req instanceof GridRestTaskRequest) {
            GridRestTaskRequest req0 = (GridRestTaskRequest) req;

            List<Object> oldParams = req0.params();

            if (oldParams != null) {
                Collection<Object> newParams = new ArrayList<>(oldParams.size());

                for (Object o : oldParams)
                    newParams.add(interceptor.onReceive(o));

                req0.params(U.sealList(newParams));
            }
        }
    }

    /**
     * Applies {@link GridClientMessageInterceptor} from {@link GridConfiguration#getClientMessageInterceptor()}
     * to all user objects in the response.
     *
     * @param res Response.
     * @param req Request.
     */
    private void interceptResponse(GridRestResponse res, GridRestRequest req) {
        GridClientMessageInterceptor interceptor = ctx.config().getClientMessageInterceptor();

        if (interceptor != null && res.getResponse() != null) {
            switch (req.command()) {
                case CACHE_GET:
                case CACHE_GET_ALL:
                case CACHE_PUT:
                case CACHE_ADD:
                case CACHE_PUT_ALL:
                case CACHE_REMOVE:
                case CACHE_REMOVE_ALL:
                case CACHE_REPLACE:
                case CACHE_INCREMENT:
                case CACHE_DECREMENT:
                case CACHE_CAS:
                case CACHE_APPEND:
                case CACHE_PREPEND:
                    res.setResponse(interceptSendObject(res.getResponse(), interceptor));

                    break;

                case EXE:
                    if (res.getResponse() instanceof GridClientTaskResultBean) {
                        GridClientTaskResultBean taskRes = (GridClientTaskResultBean) res.getResponse();

                        taskRes.setResult(interceptor.onSend(taskRes.getResult()));
                    }

                    break;

                default:
                    break;
            }
        }
    }

    /**
     * Applies interceptor to a response object.
     * Specially handler {@link Map} and {@link Collection} responses.
     *
     * @param obj Response object.
     * @param interceptor Interceptor to apply.
     * @return Intercepted object.
     */
    private static Object interceptSendObject(Object obj, GridClientMessageInterceptor interceptor) {
        if (obj instanceof Map) {
            Map<Object, Object> original = (Map<Object, Object>)obj;

            Map<Object, Object> m = new HashMap<>();

            for (Map.Entry e : original.entrySet())
                m.put(interceptor.onSend(e.getKey()), interceptor.onSend(e.getValue()));

            return m;
        }
        else if (obj instanceof Collection) {
            Collection<Object> original = (Collection<Object>)obj;

            Collection<Object> c = new ArrayList<>(original.size());

            for (Object e : original)
                c.add(interceptor.onSend(e));

            return c;
        }
        else
            return interceptor.onSend(obj);
    }

    /**
     * @param ctx Context.
     */
    public GridRestProcessor(GridKernalContext ctx) {
        super(ctx);

        securityInterceptor = ctx.config().getSecurityInterceptor();
    }

    /**
     * Authenticates remote client.
     *
     * @param req Request to authenticate.
     * @return Authentication subject context.
     * @throws GridException If authentication failed.
     */
    private Object authenticate(GridRestRequest req) throws GridException {
        UUID clientId = req.clientId();

        byte[] clientIdBytes = clientId != null ? U.uuidToBytes(clientId) : EMPTY_ID;

        byte[] sesTok = req.sessionToken();

        // Validate session.
        if (sesTok != null) {
            // Session is still valid.
            GridSecureSession ses = ctx.secureSession().validateSession(REMOTE_CLIENT, clientId, sesTok, null);

            if (ses != null)
                // Session is still valid.
                return ses.authenticationSubjectContext();
        }

        // Authenticate client if invalid session.
        Object subjCtx = ctx.auth().authenticate(REMOTE_CLIENT, clientIdBytes, req.credentials());

        if (subjCtx == null) {
            if (req.credentials() == null)
                throw new GridException("Failed to authenticate remote client (secure session SPI not set?): " + req);
            else
                throw new GridException("Failed to authenticate remote client (invalid credentials?): " + req);
        }

        return subjCtx;
    }

    /**
     * Update session token to actual state.
     *
     * @param req Grid est request.
     * @param subjCtx Authentication subject context.
     * @return Valid session token.
     * @throws GridException If session token update process failed.
     */
    private byte[] updateSessionToken(GridRestRequest req, Object subjCtx) throws GridException {
        // Update token from request to actual state.
        byte[] sesTok = ctx.secureSession().updateSession(REMOTE_CLIENT, req.clientId(), subjCtx, null);

        // Validate token has been created.
        if (sesTok == null)
            throw new GridException("Cannot create session token (is secure session SPI set?).");

        return sesTok;
    }

    /**
     *
     * @return Whether or not REST is enabled.
     */
    private boolean isRestEnabled() {
        return !ctx.config().isDaemon() && ctx.config().isRestEnabled();
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (isRestEnabled()) {
            // Register handlers.
            addHandler(new GridCacheCommandHandler(ctx));
            addHandler(new GridTaskCommandHandler(ctx));
            addHandler(new GridTopologyCommandHandler(ctx));
            addHandler(new GridVersionCommandHandler(ctx));
            addHandler(new GridLogCommandHandler(ctx));

            // Start protocol.
            startProtocol(new GridJettyRestProtocol(ctx));
            startProtocol(new GridTcpRestProtocol(ctx));
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        if (isRestEnabled()) {
            startLatch.countDown();

            if (log.isDebugEnabled())
                log.debug("REST processor started.");
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (isRestEnabled()) {
            for (GridRestProtocol proto : protos)
                proto.stop();

            // Safety.
            startLatch.countDown();

            if (log.isDebugEnabled())
                log.debug("REST processor stopped.");
        }
    }

    /**
     * @param hnd Command handler.
     */
    private void addHandler(GridRestCommandHandler hnd) {
        assert !handlers.containsValue(hnd);

        if (log.isDebugEnabled())
            log.debug("Added REST command handler: " + hnd);

        for (GridRestCommand cmd : hnd.supportedCommands()) {
            assert !handlers.containsKey(cmd);

            handlers.put(cmd, hnd);
        }
    }

    /**
     * @param proto Protocol.
     * @throws GridException If protocol initialization failed.
     */
    private void startProtocol(GridRestProtocol proto) throws GridException {
        assert !protos.contains(proto);

        protos.add(proto);

        proto.start(protoHnd);

        if (log.isDebugEnabled())
            log.debug("Added REST protocol: " + proto);
    }

    /** {@inheritDoc} */
    @Override public void addAttributes(Map<String, Object> attrs)  throws GridException {
        for (GridRestProtocol proto : protos) {
            for (GridBiTuple<String, Object> p : proto.getProperties()) {
                String key = p.getKey();

                if (key == null)
                    continue;

                if (attrs.containsKey(key))
                    throw new GridException(
                        "Node attribute collision for attribute [processor=GridRestProcessor, attr=" + key + ']');

                attrs.put(key, p.getValue());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> REST processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>   protosSize: " + protos.size());
        X.println(">>>   handlersSize: " + handlers.size());
    }
}
