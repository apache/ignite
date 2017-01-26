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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorMessageInterceptor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.rest.client.message.GridClientTaskResultBean;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.cache.GridCacheCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.datastructures.DataStructuresCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.log.GridLogCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.query.QueryCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.task.GridTaskCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.top.GridTopologyCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.version.GridVersionCommandHandler;
import org.apache.ignite.internal.processors.rest.protocols.tcp.GridTcpRestProtocol;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestTaskRequest;
import org.apache.ignite.internal.processors.rest.request.RestQueryRequest;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerFuture;
import org.apache.ignite.internal.visor.util.VisorClusterGroupEmptyException;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.thread.IgniteThread;
import org.jsr166.LongAdder8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REST_START_ON_CLIENT;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_AUTH_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SECURITY_CHECK_FAILED;
import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;

/**
 * Rest processor implementation.
 */
public class GridRestProcessor extends GridProcessorAdapter {
    /** HTTP protocol class name. */
    private static final String HTTP_PROTO_CLS =
        "org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyRestProtocol";

    /** Delay between sessions timeout checks. */
    private static final int SES_TIMEOUT_CHECK_DELAY = 1_000;

    /** Default session timout. */
    private static final int DEFAULT_SES_TIMEOUT = 30_000;

    /** Protocols. */
    private final Collection<GridRestProtocol> protos = new ArrayList<>();

    /** Command handlers. */
    protected final Map<GridRestCommand, GridRestCommandHandler> handlers = new EnumMap<>(GridRestCommand.class);

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** Busy lock. */
    private final GridSpinReadWriteLock busyLock = new GridSpinReadWriteLock();

    /** Workers count. */
    private final LongAdder8 workersCnt = new LongAdder8();

    /** ClientId-SessionId map. */
    private final ConcurrentMap<UUID, UUID> clientId2SesId = new ConcurrentHashMap<>();

    /** SessionId-Session map. */
    private final ConcurrentMap<UUID, Session> sesId2Ses = new ConcurrentHashMap<>();

    /** */
    private final Thread sesTimeoutCheckerThread;

    /** Protocol handler. */
    private final GridRestProtocolHandler protoHnd = new GridRestProtocolHandler() {
        @Override public GridRestResponse handle(GridRestRequest req) throws IgniteCheckedException {
            return handleAsync(req).get();
        }

        @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
            return handleAsync0(req);
        }
    };

    /** Session time to live. */
    private final long sesTtl;

    /**
     * @param req Request.
     * @return Future.
     */
    private IgniteInternalFuture<GridRestResponse> handleAsync0(final GridRestRequest req) {
        if (!busyLock.tryReadLock())
            return new GridFinishedFuture<>(
                new IgniteCheckedException("Failed to handle request (received request while stopping grid)."));

        try {
            final GridWorkerFuture<GridRestResponse> fut = new GridWorkerFuture<>();

            workersCnt.increment();

            GridWorker w = new GridWorker(ctx.gridName(), "rest-proc-worker", log) {
                @Override protected void body() {
                    try {
                        IgniteInternalFuture<GridRestResponse> res = handleRequest(req);

                        res.listen(new IgniteInClosure<IgniteInternalFuture<GridRestResponse>>() {
                            @Override public void apply(IgniteInternalFuture<GridRestResponse> f) {
                                try {
                                    fut.onDone(f.get());
                                }
                                catch (IgniteCheckedException e) {
                                    fut.onDone(e);
                                }
                            }
                        });
                    }
                    catch (Throwable e) {
                        if (e instanceof Error)
                            U.error(log, "Client request execution failed with error.", e);

                        fut.onDone(U.cast(e));

                        if (e instanceof Error)
                            throw e;
                    }
                    finally {
                        workersCnt.decrement();
                    }
                }
            };

            fut.setWorker(w);

            try {
                ctx.getRestExecutorService().execute(w);
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
    private IgniteInternalFuture<GridRestResponse> handleRequest(final GridRestRequest req) {
        if (startLatch.getCount() > 0) {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
                return new GridFinishedFuture<>(new IgniteCheckedException("Failed to handle request " +
                    "(protocol handler was interrupted when awaiting grid start).", e));
            }
        }

        if (log.isDebugEnabled())
            log.debug("Received request from client: " + req);

        if (ctx.security().enabled()) {
            Session ses;

            try {
                ses = session(req);
            }
            catch (IgniteCheckedException e) {
                GridRestResponse res = new GridRestResponse(STATUS_FAILED, e.getMessage());

                return new GridFinishedFuture<>(res);
            }

            assert ses != null;

            req.clientId(ses.clientId);
            req.sessionToken(U.uuidToBytes(ses.sesId));

            if (log.isDebugEnabled())
                log.debug("Next clientId and sessionToken were extracted according to request: " +
                    "[clientId="+req.clientId()+", sesTok="+Arrays.toString(req.sessionToken())+"]");

            SecurityContext secCtx0 = ses.secCtx;

            try {
                if (secCtx0 == null)
                    ses.secCtx = secCtx0 = authenticate(req);

                authorize(req, secCtx0);
            }
            catch (SecurityException e) {
                assert secCtx0 != null;

                GridRestResponse res = new GridRestResponse(STATUS_SECURITY_CHECK_FAILED, e.getMessage());

                return new GridFinishedFuture<>(res);
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(new GridRestResponse(STATUS_AUTH_FAILED, e.getMessage()));
            }
        }

        interceptRequest(req);

        GridRestCommandHandler hnd = handlers.get(req.command());

        IgniteInternalFuture<GridRestResponse> res = hnd == null ? null : hnd.handleAsync(req);

        if (res == null)
            return new GridFinishedFuture<>(
                new IgniteCheckedException("Failed to find registered handler for command: " + req.command()));

        return res.chain(new C1<IgniteInternalFuture<GridRestResponse>, GridRestResponse>() {
            @Override public GridRestResponse apply(IgniteInternalFuture<GridRestResponse> f) {
                GridRestResponse res;

                boolean failed = false;

                try {
                    res = f.get();
                }
                catch (Exception e) {
                    failed = true;

                    if (!X.hasCause(e, VisorClusterGroupEmptyException.class))
                        LT.error(log, e, "Failed to handle request: " + req.command());

                    if (log.isDebugEnabled())
                        log.debug("Failed to handle request [req=" + req + ", e=" + e + "]");

                    res = new GridRestResponse(STATUS_FAILED, e.getMessage());
                }

                assert res != null;

                if (ctx.security().enabled() && !failed)
                    res.sessionTokenBytes(req.sessionToken());

                interceptResponse(res, req);

                return res;
            }
        });
    }

    /**
     * @param req Request.
     * @return Not null session.
     * @throws IgniteCheckedException If failed.
     */
    private Session session(final GridRestRequest req) throws IgniteCheckedException {
        final UUID clientId = req.clientId();
        final byte[] sesTok = req.sessionToken();

        while (true) {
            if (F.isEmpty(sesTok) && clientId == null) {
                Session ses = Session.random();

                UUID oldSesId = clientId2SesId.put(ses.clientId, ses.sesId);

                assert oldSesId == null : "Got an illegal state for request: " + req;

                Session oldSes = sesId2Ses.put(ses.sesId, ses);

                assert oldSes == null : "Got an illegal state for request: " + req;

                return ses;
            }

            if (F.isEmpty(sesTok) && clientId != null) {
                UUID sesId = clientId2SesId.get(clientId);

                if (sesId == null) {
                    Session ses = Session.fromClientId(clientId);

                    if (clientId2SesId.putIfAbsent(ses.clientId, ses.sesId) != null)
                        continue; // Another thread already register session with the clientId.

                    Session prevSes = sesId2Ses.put(ses.sesId, ses);

                    assert prevSes == null : "Got an illegal state for request: " + req;

                    return ses;
                }
                else {
                    Session ses = sesId2Ses.get(sesId);

                    if (ses == null || !ses.touch())
                        continue; // Need to wait while timeout thread complete removing of timed out sessions.

                    return ses;
                }
            }

            if (!F.isEmpty(sesTok) && clientId == null) {
                UUID sesId = U.bytesToUuid(sesTok, 0);

                Session ses = sesId2Ses.get(sesId);

                if (ses == null)
                    throw new IgniteCheckedException("Failed to handle request - unknown session token " +
                        "(maybe expired session) [sesTok=" + U.byteArray2HexString(sesTok) + "]");

                if (!ses.touch())
                    continue; // Need to wait while timeout thread complete removing of timed out sessions.

                return ses;
            }

            if (!F.isEmpty(sesTok) && clientId != null) {
                UUID sesId = clientId2SesId.get(clientId);

                if (sesId == null || !sesId.equals(U.bytesToUuid(sesTok, 0)))
                    throw new IgniteCheckedException("Failed to handle request - unsupported case (misamatched " +
                        "clientId and session token) [clientId=" + clientId + ", sesTok=" +
                        U.byteArray2HexString(sesTok) + "]");

                Session ses = sesId2Ses.get(sesId);

                if (ses == null)
                    throw new IgniteCheckedException("Failed to handle request - unknown session token " +
                        "(maybe expired session) [sesTok=" + U.byteArray2HexString(sesTok) + "]");

                if (!ses.touch())
                    continue; // Need to wait while timeout thread complete removing of timed out sessions.

                return ses;
            }

            assert false : "Got an unreachable state.";
        }
    }

    /**
     * @param ctx Context.
     */
    public GridRestProcessor(GridKernalContext ctx) {
        super(ctx);

        long sesExpTime0;
        String sesExpTime = null;

        try {
            sesExpTime = System.getProperty(IgniteSystemProperties.IGNITE_REST_SESSION_TIMEOUT);

            if (sesExpTime != null)
                sesExpTime0 = Long.valueOf(sesExpTime) * 1000;
            else
                sesExpTime0 = DEFAULT_SES_TIMEOUT;
        }
        catch (NumberFormatException ignore) {
            U.warn(log, "Failed parsing IGNITE_REST_SESSION_TIMEOUT system variable [IGNITE_REST_SESSION_TIMEOUT="
                + sesExpTime + "]");

            sesExpTime0 = DEFAULT_SES_TIMEOUT;
        }

        sesTtl = sesExpTime0;

        sesTimeoutCheckerThread = new IgniteThread(ctx.gridName(), "session-timeout-worker",
            new GridWorker(ctx.gridName(), "session-timeout-worker", log) {
                @Override protected void body() throws InterruptedException {
                    while (!isCancelled()) {
                        Thread.sleep(SES_TIMEOUT_CHECK_DELAY);

                        for (Map.Entry<UUID, Session> e : sesId2Ses.entrySet()) {
                            Session ses = e.getValue();

                            if (ses.isTimedOut(sesTtl)) {
                                sesId2Ses.remove(ses.sesId, ses);

                                clientId2SesId.remove(ses.clientId, ses.sesId);
                            }
                        }
                    }
                }
            });
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (isRestEnabled()) {
            if (notStartOnClient()) {
                U.quietAndInfo(log, "REST protocols do not start on client node. " +
                    "To start the protocols on client node set '-DIGNITE_REST_START_ON_CLIENT=true' system property.");

                return;
            }

            // Register handlers.
            addHandler(new GridCacheCommandHandler(ctx));
            addHandler(new GridTaskCommandHandler(ctx));
            addHandler(new GridTopologyCommandHandler(ctx));
            addHandler(new GridVersionCommandHandler(ctx));
            addHandler(new DataStructuresCommandHandler(ctx));
            addHandler(new QueryCommandHandler(ctx));
            addHandler(new GridLogCommandHandler(ctx));


            // Start protocols.
            startTcpProtocol();
            startHttpProtocol();

            for (GridRestProtocol proto : protos) {
                Collection<IgniteBiTuple<String, Object>> props = proto.getProperties();

                if (props != null) {
                    for (IgniteBiTuple<String, Object> p : props) {
                        String key = p.getKey();

                        if (key == null)
                            continue;

                        if (ctx.hasNodeAttribute(key))
                            throw new IgniteCheckedException(
                                "Node attribute collision for attribute [processor=GridRestProcessor, attr=" + key + ']');

                        ctx.addNodeAttribute(key, p.getValue());
                    }
                }
            }
        }
    }

    /**
     * @return {@code True} if rest processor should not start on client node.
     */
    private boolean notStartOnClient() {
        return ctx.clientNode() && !IgniteSystemProperties.getBoolean(IGNITE_REST_START_ON_CLIENT);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        if (isRestEnabled()) {
            for (GridRestProtocol proto : protos)
                proto.onKernalStart();

            sesTimeoutCheckerThread.setDaemon(true);

            sesTimeoutCheckerThread.start();

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

            U.interrupt(sesTimeoutCheckerThread);

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

    /**
     * Applies {@link ConnectorMessageInterceptor}
     * from {@link ConnectorConfiguration#getMessageInterceptor()} ()}
     * to all user parameters in the request.
     *
     * @param req Client request.
     */
    private void interceptRequest(GridRestRequest req) {
        ConnectorMessageInterceptor interceptor = config().getMessageInterceptor();

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
     * Applies {@link ConnectorMessageInterceptor} from
     * {@link ConnectorConfiguration#getMessageInterceptor()}
     * to all user objects in the response.
     *
     * @param res Response.
     * @param req Request.
     */
    private void interceptResponse(GridRestResponse res, GridRestRequest req) {
        ConnectorMessageInterceptor interceptor = config().getMessageInterceptor();

        if (interceptor != null && res.getResponse() != null) {
            switch (req.command()) {
                case CACHE_CONTAINS_KEYS:
                case CACHE_CONTAINS_KEY:
                case CACHE_GET:
                case CACHE_GET_ALL:
                case CACHE_PUT:
                case CACHE_ADD:
                case CACHE_PUT_ALL:
                case CACHE_REMOVE:
                case CACHE_REMOVE_ALL:
                case CACHE_REPLACE:
                case ATOMIC_INCREMENT:
                case ATOMIC_DECREMENT:
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
    }    /**
     * Applies interceptor to a response object.
     * Specially handler {@link Map} and {@link Collection} responses.
     *
     * @param obj Response object.
     * @param interceptor Interceptor to apply.
     * @return Intercepted object.
     */
    private static Object interceptSendObject(Object obj, ConnectorMessageInterceptor interceptor) {
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
     * @throws IgniteCheckedException If authentication failed.
     */
    private SecurityContext authenticate(GridRestRequest req) throws IgniteCheckedException {
        assert req.clientId() != null;

        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.subjectType(REMOTE_CLIENT);
        authCtx.subjectId(req.clientId());
        authCtx.nodeAttributes(Collections.<String, Object>emptyMap());

        SecurityCredentials cred;

        if (req.credentials() instanceof SecurityCredentials)
            cred = (SecurityCredentials)req.credentials();
        else if (req.credentials() instanceof String) {
            String credStr = (String)req.credentials();

            int idx = credStr.indexOf(':');

            cred = idx >= 0 && idx < credStr.length() ?
                new SecurityCredentials(credStr.substring(0, idx), credStr.substring(idx + 1)) :
                new SecurityCredentials(credStr, null);
        }
        else {
            cred = new SecurityCredentials();

            cred.setUserObject(req.credentials());
        }

        authCtx.address(req.address());

        authCtx.credentials(cred);

        SecurityContext subjCtx = ctx.security().authenticate(authCtx);

        if (subjCtx == null) {
            if (req.credentials() == null)
                throw new IgniteCheckedException("Failed to authenticate remote client (secure session SPI not set?): " + req);
            else
                throw new IgniteCheckedException("Failed to authenticate remote client (invalid credentials?): " + req);
        }

        return subjCtx;
    }

    /**
     * @param req REST request.
     * @param sCtx Security context.
     * @throws SecurityException If authorization failed.
     */
    private void authorize(GridRestRequest req, SecurityContext sCtx) throws SecurityException {
        SecurityPermission perm = null;
        String name = null;

        switch (req.command()) {
            case CACHE_GET:
            case CACHE_CONTAINS_KEY:
            case CACHE_CONTAINS_KEYS:
            case CACHE_GET_ALL:
                perm = SecurityPermission.CACHE_READ;
                name = ((GridRestCacheRequest)req).cacheName();

                break;

            case EXECUTE_SQL_QUERY:
            case EXECUTE_SQL_FIELDS_QUERY:
            case EXECUTE_SCAN_QUERY:
            case CLOSE_SQL_QUERY:
            case FETCH_SQL_QUERY:
                perm = SecurityPermission.CACHE_READ;
                name = ((RestQueryRequest)req).cacheName();

                break;

            case CACHE_PUT:
            case CACHE_ADD:
            case CACHE_PUT_ALL:
            case CACHE_REPLACE:
            case CACHE_CAS:
            case CACHE_APPEND:
            case CACHE_PREPEND:
            case CACHE_GET_AND_PUT:
            case CACHE_GET_AND_REPLACE:
            case CACHE_GET_AND_PUT_IF_ABSENT:
            case CACHE_PUT_IF_ABSENT:
            case CACHE_REPLACE_VALUE:
                perm = SecurityPermission.CACHE_PUT;
                name = ((GridRestCacheRequest)req).cacheName();

                break;

            case CACHE_REMOVE:
            case CACHE_REMOVE_ALL:
            case CACHE_GET_AND_REMOVE:
            case CACHE_REMOVE_VALUE:
                perm = SecurityPermission.CACHE_REMOVE;
                name = ((GridRestCacheRequest)req).cacheName();

                break;

            case EXE:
            case RESULT:
                perm = SecurityPermission.TASK_EXECUTE;
                name = ((GridRestTaskRequest)req).taskName();

                break;

            case GET_OR_CREATE_CACHE:
            case DESTROY_CACHE:
                perm = SecurityPermission.ADMIN_CACHE;
                name = ((GridRestCacheRequest)req).cacheName();

                break;

            case CACHE_METRICS:
            case CACHE_SIZE:
            case CACHE_METADATA:
            case TOPOLOGY:
            case NODE:
            case VERSION:
            case NOOP:
            case QUIT:
            case ATOMIC_INCREMENT:
            case ATOMIC_DECREMENT:
            case NAME:
            case LOG:
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
        return !ctx.config().isDaemon() && ctx.config().getConnectorConfiguration() != null;
    }

    /**
     * @param hnd Command handler.
     */
    private void addHandler(GridRestCommandHandler hnd) {
        assert !handlers.containsValue(hnd);

        if (log.isDebugEnabled())
            log.debug("Added REST command handler: " + hnd);

        for (GridRestCommand cmd : hnd.supportedCommands()) {
            assert !handlers.containsKey(cmd) : cmd;

            handlers.put(cmd, hnd);
        }
    }

    /**
     * Starts TCP protocol.
     *
     * @throws IgniteCheckedException In case of error.
     */
    private void startTcpProtocol() throws IgniteCheckedException {
        startProtocol(new GridTcpRestProtocol(ctx));
    }

    /**
     * Starts HTTP protocol if it exists on classpath.
     *
     * @throws IgniteCheckedException In case of error.
     */
    private void startHttpProtocol() throws IgniteCheckedException {
        try {
            Class<?> cls = Class.forName(HTTP_PROTO_CLS);

            Constructor<?> ctor = cls.getConstructor(GridKernalContext.class);

            GridRestProtocol proto = (GridRestProtocol)ctor.newInstance(ctx);

            startProtocol(proto);
        }
        catch (ClassNotFoundException ignored) {
            if (log.isDebugEnabled())
                log.debug("Failed to initialize HTTP REST protocol (consider adding ignite-rest-http " +
                    "module to classpath).");
        }
        catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new IgniteCheckedException("Failed to initialize HTTP REST protocol.", e);
        }
    }

    /**
     * @return Client configuration.
     */
    private ConnectorConfiguration config() {
        return ctx.config().getConnectorConfiguration();
    }

    /**
     * @param proto Protocol.
     * @throws IgniteCheckedException If protocol initialization failed.
     */
    private void startProtocol(GridRestProtocol proto) throws IgniteCheckedException {
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

    /**
     * Session.
     */
    private static class Session {
        /** Expiration flag. It's a final state of lastToucnTime. */
        private static final Long TIMEDOUT_FLAG = 0L;

        /** Client id. */
        private final UUID clientId;

        /** Session token id. */
        private final UUID sesId;

        /**
         * Time when session is used last time.
         * If this time was set at TIMEDOUT_FLAG, then it should never be changed.
         */
        private final AtomicLong lastTouchTime = new AtomicLong(U.currentTimeMillis());

        /** Security context. */
        private volatile SecurityContext secCtx;

        /**
         * @param clientId Client ID.
         * @param sesId session ID.
         */
        private Session(UUID clientId, UUID sesId) {
            this.clientId = clientId;
            this.sesId = sesId;
        }

        /**
         * Static constructor.
         *
         * @return New session instance with random client ID and random session ID.
         */
        static Session random() {
            return new Session(UUID.randomUUID(), UUID.randomUUID());
        }

        /**
         * Static constructor.
         *
         * @param clientId Client ID.
         * @return New session instance with given client ID and random session ID.
         */
        static Session fromClientId(UUID clientId) {
            return new Session(clientId, UUID.randomUUID());
        }

        /**
         * Static constructor.
         *
         * @param sesTokId Session token ID.
         * @return New session instance with random client ID and given session ID.
         */
        static Session fromSessionToken(UUID sesTokId) {
            return new Session(UUID.randomUUID(), sesTokId);
        }

        /**
         * Checks expiration of session and if expired then sets TIMEDOUT_FLAG.
         *
         * @param sesTimeout Session timeout.
         * @return <code>True</code> if expired.
         * @see #touch()
         */
        boolean isTimedOut(long sesTimeout) {
            long time0 = lastTouchTime.get();

            if (time0 == TIMEDOUT_FLAG)
                return true;

            return U.currentTimeMillis() - time0 > sesTimeout && lastTouchTime.compareAndSet(time0, TIMEDOUT_FLAG);
        }

        /**
         * Checks whether session at expired state (EPIRATION_FLAG) or not, if not then tries to update last touch time.
         *
         * @return {@code False} if session timed out (not successfully touched).
         * @see #isTimedOut(long)
         */
        boolean touch() {
            while (true) {
                long time0 = lastTouchTime.get();

                if (time0 == TIMEDOUT_FLAG)
                    return false;

                boolean success = lastTouchTime.compareAndSet(time0, U.currentTimeMillis());

                if (success)
                    return true;
            }
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof Session))
                return false;

            Session ses = (Session)o;

            if (clientId != null ? !clientId.equals(ses.clientId) : ses.clientId != null)
                return false;

            if (sesId != null ? !sesId.equals(ses.sesId) : ses.sesId != null)
                return false;

            return true;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = clientId != null ? clientId.hashCode() : 0;

            res = 31 * res + (sesId != null ? sesId.hashCode() : 0);

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Session.class, this);
        }
    }
}
