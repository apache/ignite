/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.securesession.*;
import org.gridgain.grid.kernal.managers.security.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.kernal.processors.rest.handlers.cache.*;
import org.gridgain.grid.kernal.processors.rest.handlers.log.*;
import org.gridgain.grid.kernal.processors.rest.handlers.metadata.*;
import org.gridgain.grid.kernal.processors.rest.handlers.task.*;
import org.gridgain.grid.kernal.processors.rest.handlers.top.*;
import org.gridgain.grid.kernal.processors.rest.handlers.version.*;
import org.gridgain.grid.kernal.processors.rest.protocols.tcp.*;
import org.gridgain.grid.kernal.processors.rest.request.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.spi.authentication.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jdk8.backport.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.kernal.processors.rest.GridRestResponse.*;
import static org.gridgain.grid.security.GridSecuritySubjectType.*;

/**
 * Rest processor implementation.
 */
public class GridRestProcessor extends GridProcessorAdapter {
    /** HTTP protocol class name. */
    private static final String HTTP_PROTO_CLS =
        "org.gridgain.grid.kernal.processors.rest.protocols.http.jetty.GridJettyRestProtocol";

    /** Protocols. */
    private final Collection<GridRestProtocol> protos = new ArrayList<>();

    /** Command handlers. */
    protected final Map<GridRestCommand, GridRestCommandHandler> handlers = new EnumMap<>(GridRestCommand.class);

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** Busy lock. */
    private final GridSpinReadWriteLock busyLock = new GridSpinReadWriteLock();

    /** Workers count. */
    private final LongAdder workersCnt = new LongAdder();

    /** Protocol handler. */
    private final GridRestProtocolHandler protoHnd = new GridRestProtocolHandler() {
        @Override public GridRestResponse handle(GridRestRequest req) throws GridException {
            return handleAsync(req).get();
        }

        @Override public GridFuture<GridRestResponse> handleAsync(GridRestRequest req) {
            return handleAsync0(req);
        }
    };

    /**
     * @param req Request.
     * @return Future.
     */
    private GridFuture<GridRestResponse> handleAsync0(final GridRestRequest req) {
        if (!busyLock.tryReadLock())
            return new GridFinishedFuture<>(ctx,
                new GridException("Failed to handle request (received request while stopping grid)."));

        try {
            final GridWorkerFuture<GridRestResponse> fut = new GridWorkerFuture<>(ctx);

            workersCnt.increment();

            GridWorker w = new GridWorker(ctx.gridName(), "rest-proc-worker", log) {
                @Override protected void body() {
                    try {
                        GridFuture<GridRestResponse> res = handleRequest(req);

                        res.listenAsync(new IgniteInClosure<GridFuture<GridRestResponse>>() {
                            @Override public void apply(GridFuture<GridRestResponse> f) {
                                try {
                                    fut.onDone(f.get());
                                }
                                catch (GridException e) {
                                    fut.onDone(e);
                                }
                            }
                        });
                    }
                    catch (Throwable e) {
                        if (e instanceof Error)
                            U.error(log, "Client request execution failed with error.", e);

                        fut.onDone(U.cast(e));
                    }
                    finally {
                        workersCnt.decrement();
                    }
                }
            };

            fut.setWorker(w);

            try {
                config().getRestExecutorService().execute(w);
            }
            catch (RejectedExecutionException e) {
                U.error(log, "Failed to execute worker due to execution rejection " +
                    "(increase upper bound on REST executor service). " +
                    "Will attempt to process request in the current thread instead.", e);

                w.run();
            }

            return fut;
        }
        finally {
            busyLock.readUnlock();
        }
    }

    /**
     * @param req Request.
     * @return Future.
     */
    private GridFuture<GridRestResponse> handleRequest(final GridRestRequest req) {
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

        GridSecurityContext subjCtx = null;

        try {
            subjCtx = authenticate(req);

            authorize(req, subjCtx);
        }
        catch (GridSecurityException e) {
            assert subjCtx != null;

            GridRestResponse res = new GridRestResponse(STATUS_SECURITY_CHECK_FAILED, e.getMessage());

            if (ctx.isEnterprise()) {
                try {
                    res.sessionTokenBytes(updateSessionToken(req, subjCtx));
                }
                catch (GridException e1) {
                    U.warn(log, "Cannot update response session token: " + e1.getMessage());
                }
            }

            return new GridFinishedFuture<>(ctx, res);
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(ctx, new GridRestResponse(STATUS_AUTH_FAILED, e.getMessage()));
        }

        interceptRequest(req);

        GridRestCommandHandler hnd = handlers.get(req.command());

        GridFuture<GridRestResponse> res = hnd == null ? null : hnd.handleAsync(req);

        if (res == null)
            return new GridFinishedFuture<>(ctx,
                new GridException("Failed to find registered handler for command: " + req.command()));

        final GridSecurityContext subjCtx0 = subjCtx;

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

                if (ctx.secureSession().enabled()) {
                    try {
                        res.sessionTokenBytes(updateSessionToken(req, subjCtx0));
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

    /**
     * @param ctx Context.
     */
    public GridRestProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (isRestEnabled()) {
            // Register handlers.
            addHandler(new GridCacheCommandHandler(ctx));
            addHandler(new GridCacheQueryCommandHandler(ctx));
            addHandler(new GridTaskCommandHandler(ctx));
            addHandler(new GridTopologyCommandHandler(ctx));
            addHandler(new GridVersionCommandHandler(ctx));
            addHandler(new GridLogCommandHandler(ctx));
            addHandler(new GridPortableMetadataHandler(ctx));

            // Start protocols.
            startTcpProtocol();
            startHttpProtocol();
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        if (isRestEnabled()) {
            for (GridRestProtocol proto : protos)
                proto.onKernalStart();

            startLatch.countDown();

            if (log.isDebugEnabled())
                log.debug("REST processor started.");
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override public void onKernalStop(boolean cancel) {
        if (isRestEnabled()) {
            busyLock.writeLock();

            boolean interrupted = Thread.interrupted();

            while (workersCnt.sum() != 0) {
                try {
                    Thread.sleep(200);
                }
                catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }

            if (interrupted)
                Thread.currentThread().interrupt();

            for (GridRestProtocol proto : protos)
                proto.stop();

            // Safety.
            startLatch.countDown();

            if (log.isDebugEnabled())
                log.debug("REST processor stopped.");
        }
    }

    /** {@inheritDoc} */
    @Override public void addAttributes(Map<String, Object> attrs)  throws GridException {
        for (GridRestProtocol proto : protos) {
            Collection<IgniteBiTuple<String, Object>> props = proto.getProperties();

            if (props != null) {
                for (IgniteBiTuple<String, Object> p : props) {
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
    }

    /**
     * Applies {@link GridClientMessageInterceptor}
     * from {@link GridClientConnectionConfiguration#getClientMessageInterceptor()}
     * to all user parameters in the request.
     *
     * @param req Client request.
     */
    private void interceptRequest(GridRestRequest req) {
        GridClientMessageInterceptor interceptor = config().getClientMessageInterceptor();

        if (interceptor == null)
            return;

        if (req instanceof GridRestCacheRequest) {
            GridRestCacheRequest req0 = (GridRestCacheRequest) req;

            req0.key(interceptor.onReceive(req0.key()));
            req0.value(interceptor.onReceive(req0.value()));
            req0.value2(interceptor.onReceive(req0.value2()));

            Map<Object, Object> oldVals = req0.values();

            if (oldVals != null) {
                Map<Object, Object> newVals = U.newHashMap(oldVals.size());

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
     * Applies {@link GridClientMessageInterceptor} from
     * {@link GridClientConnectionConfiguration#getClientMessageInterceptor()}
     * to all user objects in the response.
     *
     * @param res Response.
     * @param req Request.
     */
    private void interceptResponse(GridRestResponse res, GridRestRequest req) {
        GridClientMessageInterceptor interceptor = config().getClientMessageInterceptor();

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
                        GridClientTaskResultBean taskRes = (GridClientTaskResultBean)res.getResponse();

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
     * Authenticates remote client.
     *
     * @param req Request to authenticate.
     * @return Authentication subject context.
     * @throws GridException If authentication failed.
     */
    private GridSecurityContext authenticate(GridRestRequest req) throws GridException {
        UUID clientId = req.clientId();

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
        GridAuthenticationContextAdapter authCtx = new GridAuthenticationContextAdapter();

        authCtx.subjectType(REMOTE_CLIENT);
        authCtx.subjectId(req.clientId());

        GridSecurityCredentials cred;

        if (req.credentials() instanceof GridSecurityCredentials)
            cred = (GridSecurityCredentials)req.credentials();
        else if (req.credentials() instanceof String) {
            String credStr = (String)req.credentials();

            int idx = credStr.indexOf(':');

            cred = idx >= 0 && idx < credStr.length() ?
                new GridSecurityCredentials(credStr.substring(0, idx), credStr.substring(idx + 1)) :
                new GridSecurityCredentials(credStr, null);
        }
        else {
            cred = new GridSecurityCredentials();

            cred.setUserObject(req.credentials());
        }

        authCtx.address(req.address());

        authCtx.credentials(cred);

        GridSecurityContext subjCtx = ctx.security().authenticate(authCtx);

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
    private byte[] updateSessionToken(GridRestRequest req, GridSecurityContext subjCtx) throws GridException {
        // Update token from request to actual state.
        byte[] sesTok = ctx.secureSession().updateSession(REMOTE_CLIENT, req.clientId(), subjCtx, null);

        // Validate token has been created.
        if (sesTok == null)
            throw new GridException("Cannot create session token (is secure session SPI set?).");

        return sesTok;
    }

    /**
     * @param req REST request.
     * @param sCtx Security context.
     * @throws GridSecurityException If authorization failed.
     */
    private void authorize(GridRestRequest req, GridSecurityContext sCtx) throws GridSecurityException {
        GridSecurityPermission perm = null;
        String name = null;

        switch (req.command()) {
            case CACHE_GET:
            case CACHE_GET_ALL:
                perm = GridSecurityPermission.CACHE_READ;
                name = ((GridRestCacheRequest)req).cacheName();

                break;

            case CACHE_QUERY_EXECUTE:
            case CACHE_QUERY_FETCH:
            case CACHE_QUERY_REBUILD_INDEXES:
                perm = GridSecurityPermission.CACHE_READ;
                name = ((GridRestCacheQueryRequest)req).cacheName();

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
                perm = GridSecurityPermission.CACHE_PUT;
                name = ((GridRestCacheRequest)req).cacheName();

                break;

            case CACHE_REMOVE:
            case CACHE_REMOVE_ALL:
                perm = GridSecurityPermission.CACHE_REMOVE;
                name = ((GridRestCacheRequest)req).cacheName();

                break;

            case EXE:
            case RESULT:
                perm = GridSecurityPermission.TASK_EXECUTE;
                name = ((GridRestTaskRequest)req).taskName();

                break;

            case CACHE_METRICS:
            case TOPOLOGY:
            case NODE:
            case VERSION:
            case LOG:
            case NOOP:
            case QUIT:
            case GET_PORTABLE_METADATA:
            case PUT_PORTABLE_METADATA:
                break;

            default:
                throw new AssertionError("Unexpected command: " + req.command());
        }

        if (perm != null)
            ctx.security().authorize(name, perm, sCtx);
    }

    /**
     *
     * @return Whether or not REST is enabled.
     */
    private boolean isRestEnabled() {
        return !ctx.config().isDaemon() && ctx.config().getClientConnectionConfiguration() != null;
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
     * Starts TCP protocol.
     *
     * @throws GridException In case of error.
     */
    private void startTcpProtocol() throws GridException {
        startProtocol(new GridTcpRestProtocol(ctx));
    }

    /**
     * Starts HTTP protocol if it exists on classpath.
     *
     * @throws GridException In case of error.
     */
    private void startHttpProtocol() throws GridException {
        try {
            Class<?> cls = Class.forName(HTTP_PROTO_CLS);

            Constructor<?> ctor = cls.getConstructor(GridKernalContext.class);

            GridRestProtocol proto = (GridRestProtocol)ctor.newInstance(ctx);

            startProtocol(proto);
        }
        catch (ClassNotFoundException ignored) {
            U.quietAndWarn(log, "Failed to initialize HTTP REST protocol (consider adding gridgain-rest-http " +
                "module to classpath).");
        }
        catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new GridException("Failed to initialize HTTP REST protocol.", e);
        }
    }

    /**
     * @return Client configuration.
     */
    private GridClientConnectionConfiguration config() {
        return ctx.config().getClientConnectionConfiguration();
    }

    /**
     * @param proto Protocol.
     * @throws GridException If protocol initialization failed.
     */
    private void startProtocol(GridRestProtocol proto) throws GridException {
        assert proto != null;
        assert !protos.contains(proto);

        protos.add(proto);

        proto.start(protoHnd);

        if (log.isDebugEnabled())
            log.debug("Added REST protocol: " + proto);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> REST processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>   protosSize: " + protos.size());
        X.println(">>>   handlersSize: " + handlers.size());
    }
}
