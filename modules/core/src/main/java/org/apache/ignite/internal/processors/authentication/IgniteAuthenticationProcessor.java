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

package org.apache.ignite.internal.processors.authentication;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AuthenticationConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.AUTH_PROC;

/**
 */
public class IgniteAuthenticationProcessor extends GridProcessorAdapter implements MetastorageLifecycleListener {
    /** Store user prefix. */
    private static final String STORE_USER_PREFIX = "user.";

    /** Store users info version. */
    private static final String STORE_USERS_VERSION = "users-version";

    /** Discovery event types. */
    private static final int[] DISCO_EVT_TYPES = new int[] {EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_NODE_JOINED};

    /** Map monitor. */
    private final Object mux = new Object();

    /** User operation finish futures (Operatio ID -> fututre). */
    private final ConcurrentMap<IgniteUuid, UserOperationFinishFuture> opFinishFuts
        = new ConcurrentHashMap<>();

    /** Futures prepared user map. Authentication message ID -> public future. */
    private final ConcurrentMap<IgniteUuid, GridFutureAdapter<User>> authFuts = new ConcurrentHashMap<>();

    /** Active operations. */
    private List<UserManagementOperation> activeOperations = new ArrayList<>();

    /** User map. */
    private Map<String, User> users;

    /** Shared context. */
    @GridToStringExclude
    private GridCacheSharedContext<?, ?> sharedCtx;

    /** Meta storage. */
    private ReadWriteMetastorage metastorage;

    /** Executor. */
    private IgniteThreadPoolExecutor exec;

    /** Coordinator node. */
    private ClusterNode crdNode;

    /** Users info version. */
    private long usersInfoVersion;

    /** Is authentication enabled. */
    private boolean isAuthEnabled;

    /** Disconnected flag. */
    private boolean disconnected;

    /** Pending message of the finished operation. May be resend when coordinator node leave. */
    private UserManagementOperationFinishedMessage pendingFinishMsg;

    /** Initial users map and operations received from coordinator on the node joined to the cluster. */
    private InitialUsersData initUsrs;

    /** I/O message listener. */
    private GridMessageListener ioLsnr;

    /** System discovery message listener. */
    private DiscoveryEventListener discoLsnr;


    /**
     * @param ctx Kernal context.
     */
    public IgniteAuthenticationProcessor(GridKernalContext ctx) {
        super(ctx);

        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        GridDiscoveryManager discoMgr = ctx.discovery();
        GridIoManager ioMgr = ctx.io();

        discoMgr.setCustomEventListener(UserProposedMessage.class, new UserProposedListener());

        discoMgr.setCustomEventListener(UserAcceptedMessage.class, new UserAcceptedListener());

        AuthenticationConfiguration authCfg
            = ctx.config().getClientConnectorConfiguration().getAuthenticationConfiguration();

        isAuthEnabled = authCfg != null && authCfg.isEnabled();

        discoLsnr = new DiscoveryEventListener() {
            @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
                if (ctx.isStopping())
                    return;

                switch (evt.type()) {
                    case EVT_NODE_LEFT:
                    case EVT_NODE_FAILED:
                        onNodeLeft(evt.eventNode().id());
                        break;

                    case EVT_NODE_JOINED:
                        onNodeJoin(evt.eventNode().id());
                        break;
                }
            }
        };

        ctx.event().addDiscoveryEventListener(discoLsnr, DISCO_EVT_TYPES);

        ioLsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msg instanceof UserManagementOperationFinishedMessage)
                    onFinishMessage(nodeId, (UserManagementOperationFinishedMessage)msg);
                else if (msg instanceof UserAuthenticateRequestMessage)
                    onAuthenticateRequestMessage(nodeId, (UserAuthenticateRequestMessage)msg);
                else if (msg instanceof UserAuthenticateResponseMessage)
                    onAuthenticateResponseMessage((UserAuthenticateResponseMessage)msg);
            }
        };

        ioMgr.addMessageListener(GridTopic.TOPIC_AUTH, ioLsnr);

        synchronized (mux) {
            exec = new IgniteThreadPoolExecutor(
                "auth",
                ctx.config().getIgniteInstanceName(),
                1,
                1,
                0,
                new LinkedBlockingQueue<>());
        }
    }

    /**
     * On cache processor started.
     */
    public void cacheProcessorStarted() {
        sharedCtx = ctx.cache().context();
    }


    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        ctx.io().removeMessageListener(GridTopic.TOPIC_AUTH, ioLsnr);

        ctx.event().removeDiscoveryEventListener(discoLsnr, DISCO_EVT_TYPES);

        synchronized (mux) {
            cancelFutures("Node stopped");

            if (!cancel)
                exec.shutdown();
            else
                exec.shutdownNow();
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        cancelFutures("Kernal stopped.");
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        synchronized (mux) {
            assert !disconnected;

            disconnected = true;

            cancelFutures("Client node was disconnected from topology (operation result is unknown).");
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean active) {
        synchronized (mux) {
            assert disconnected;

            disconnected = false;
        }

        return null;
    }

    /**
     * Authenticate user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @return User object on successful authenticate. Otherwise returns {@code null}.
     * @throws IgniteCheckedException On authentication error.
     */
    public AuthorizationContext authenticate(String login, String passwd) throws IgniteCheckedException {
        checkActivate();

        if (ctx.clientNode()) {
            while (true) {
                try {
                    GridFutureAdapter<User> fut = new GridFutureAdapter<>();

                    UserAuthenticateRequestMessage msg = new UserAuthenticateRequestMessage(login, passwd);

                    authFuts.put(msg.id(), fut);

                    synchronized (mux) {
                        ctx.io().sendToGridTopic(coordinator(), GridTopic.TOPIC_AUTH, msg, GridIoPolicy.SYSTEM_POOL);
                    }

                    return new AuthorizationContext(fut.get());
                }
                catch (RetryOnCoordinatorLeftException e) {
                    // No-op.
                }
            }
        }
        else
            return new AuthorizationContext(authenticateOnServer(login, passwd));
    }

    /**
     * Adds new user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @throws IgniteAuthenticationException On error.
     */
    public void addUser(String login, String passwd) throws IgniteCheckedException {
        checkActivate();

        UserManagementOperation op = new UserManagementOperation(User.create(login, passwd),
            UserManagementOperation.OperationType.ADD);

        execUserOperation(op);
    }

    /**
     * @param login User name.
     * @throws IgniteCheckedException On error.
     */
    public void removeUser(String login) throws IgniteCheckedException {
        checkActivate();

        UserManagementOperation op = new UserManagementOperation(User.create(login),
            UserManagementOperation.OperationType.REMOVE);

        execUserOperation(op);
    }

    /**
     * @param login User name.
     * @param passwd User password.
     * @throws IgniteCheckedException On error.
     */
    public void updateUser(String login, String passwd) throws IgniteCheckedException {
        checkActivate();

        UserManagementOperation op = new UserManagementOperation(User.create(login, passwd),
            UserManagementOperation.OperationType.UPDATE);

        execUserOperation(op);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        synchronized (mux) {
            if (!ctx.clientNode()) {
                Long ver = (Long)metastorage.read(STORE_USERS_VERSION);
                usersInfoVersion = ver == null ? 0 : ver;

                users = new HashMap<>();

                Map<String, User> readUsers = (Map<String, User>)metastorage.readForPredicate(new IgnitePredicate<String>() {
                    @Override public boolean apply(String key) {
                        return key != null && key.startsWith(STORE_USER_PREFIX);
                    }
                });

                for (User u : readUsers.values())
                    users.put(u.name(), u);
            }
            else
                users = null;

            log.info("+++ ready");
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        if (!ctx.clientNode()) {
            this.metastorage = metastorage;

            if (usersInfoVersion == 0)
                addDefaultUser();
        }
        else
            this.metastorage = null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return DiscoveryDataExchangeType.AUTH_PROC;
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (!dataBag.commonDataCollectedFor(AUTH_PROC.ordinal())) {
            synchronized (mux) {
                log.info("+++ collect");
                InitialUsersData d = new InitialUsersData(users.values(), activeOperations, usersInfoVersion);

                if (log.isDebugEnabled())
                    log.debug("Collected initial users data: " + d);

                dataBag.addGridCommonData(AUTH_PROC.ordinal(), d);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        synchronized (mux) {
            initUsrs = (InitialUsersData)data.commonData();
        }
    }

    /**
     * Check cluster state.
     */
    private void checkActivate() {
        if (!ctx.state().publicApiActiveState(true)) {
            throw new IgniteException("Can not perform the operation because the cluster is inactive. Note, that " +
                "the cluster is considered inactive by default if Ignite Persistent Store is used to let all the nodes " +
                "join the cluster. To activate the cluster call Ignite.active(true).");
        }
    }

    /**
     * @throws IgniteCheckedException On error.
     */
    private void addDefaultUser() throws IgniteCheckedException {
        User u = User.defaultUser();

        metastorage.write(STORE_USER_PREFIX + u.name(), u);

        metastorage.write(STORE_USERS_VERSION, usersInfoVersion + 1);

        synchronized (mux) {
            usersInfoVersion++;

            users.put(u.name(), u);
        }
    }

    /**
     * Authenticate user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @return User object on successful authenticate. Otherwise returns {@code null}.
     * @throws IgniteCheckedException On authentication error.
     */
    private User authenticateOnServer(String login, String passwd) throws IgniteCheckedException {
        assert !ctx.clientNode() : "Must be used on server node";

        User usr;

        synchronized (mux) {
            usr = users.get(login);
        }

        if (usr == null)
            throw new UserAuthenticationException("The user name or password is incorrect. [userName=" + login + ']');

        if (usr.authorize(passwd))
            return usr;
        else
            throw new UserAuthenticationException("The user name or password is incorrect. [userName=" + login + ']');
    }

    /**
     * @param op User operation.
     * @throws IgniteCheckedException On error.
     */
    private void execUserOperation(UserManagementOperation op) throws IgniteCheckedException {
        if (isAuthEnabled) {
            AuthorizationContext actx = AuthorizationContext.context();

            if (actx == null)
                throw new IgniteAccessControlException("Operation not allowed: authorized context is empty.");

            actx.checkUserOperation(op);
        }

        UserOperationFinishFuture fut = new UserOperationFinishFuture(op);

        opFinishFuts.putIfAbsent(op.id(), fut);

        UserProposedMessage msg = new UserProposedMessage(op, ctx.localNodeId());

        ctx.discovery().sendCustomEvent(msg);

        fut.get();
    }

    /**
     * @param op The operation with users.
     * @throws IgniteAuthenticationException On authentication error.
     */
    private void processOperationLocal(UserManagementOperation op) throws IgniteCheckedException {
        assert op != null && op.user() != null : "Invalid operation: " + op;

//        log.info("+++ DO " + op);

        switch (op.type()) {
            case ADD:
                addUserLocal(op);

                break;

            case REMOVE:
                removeUserLocal(op);

                break;

            case UPDATE:
                updateUserLocal(op);

                break;
        }
    }

    /**
     * Adds new user locally.
     *
     * @param op User operation.
     * @throws IgniteAuthenticationException On error.
     */
    private void addUserLocal(final UserManagementOperation op) throws IgniteCheckedException {
        User usr = op.user();

        String userName = usr.name();

        synchronized (mux) {
            if (users.containsKey(userName))
                throw new IgniteAuthenticationException("User already exists. [login=" + userName + ']');
        }

        metastorage.write(STORE_USER_PREFIX + userName, usr);

        metastorage.write(STORE_USERS_VERSION, usersInfoVersion + 1);

        synchronized (mux) {
            usersInfoVersion++;

            activeOperations.remove(op);

            users.put(userName, usr);
        }
    }

    /**
     * Remove user from MetaStorage.
     *
     * @param op Operation.
     * @throws IgniteCheckedException On error.
     */
    private void removeUserLocal(UserManagementOperation op) throws IgniteCheckedException {
        User usr = op.user();

        synchronized (mux) {
            if (!users.containsKey(usr.name()))
                throw new IgniteAuthenticationException("User doesn't exist. [userName=" + usr.name() + ']');
        }

        metastorage.remove(STORE_USER_PREFIX + usr.name());

        metastorage.write(STORE_USERS_VERSION, usersInfoVersion + 1);

        synchronized (mux) {
            usersInfoVersion++;

            activeOperations.remove(op);

            users.remove(usr.name());
        }
    }

    /**
     * Remove user from MetaStorage.
     *
     * @param op Operation.
     * @throws IgniteCheckedException On error.
     */
    private void updateUserLocal(UserManagementOperation op) throws IgniteCheckedException {
        User usr = op.user();

        synchronized (mux) {
            if (!users.containsKey(usr.name()))
                throw new IgniteAuthenticationException("User doesn't exist. [userName=" + usr.name() + ']');
        }

        metastorage.write(STORE_USER_PREFIX + usr.name(), usr);

        metastorage.write(STORE_USERS_VERSION, usersInfoVersion + 1);

        synchronized (mux) {
            usersInfoVersion++;

            activeOperations.remove(op);

            users.put(usr.name(), usr);
        }
    }

    /**
     * Get current coordinator node.
     *
     * @return Coordinator node.
     */
    private ClusterNode coordinator() {
        synchronized (mux) {
            if (crdNode != null)
                return crdNode;
            else {
                ClusterNode res = null;

                for (ClusterNode node : ctx.discovery().aliveServerNodes()) {
                    if (res == null || res.order() > node.order())
                        res = node;
                }

                assert res != null;

                crdNode = res;

                return res;
            }
        }
    }

    /**
     * @param msg Error message.
     */
    private void cancelFutures(String msg) {
        for (UserOperationFinishFuture fut : opFinishFuts.values())
            fut.onDone(null, new IgniteFutureCancelledException(msg));

        for (GridFutureAdapter<User> fut : authFuts.values())
            fut.onDone(null, new IgniteFutureCancelledException(msg));
    }

    /**
     * @param nodeId Joined node ID.
     */
    private void onNodeJoin(UUID nodeId) {
        synchronized (mux) {
            if (!ctx.clientNode()) {
                for (UserOperationFinishFuture f : opFinishFuts.values())
                    f.onNodeJoin(nodeId);
            }
        }
    }

    /**
     * @param nodeId Left node ID.
     */
    private void onNodeLeft(UUID nodeId) {
        synchronized (mux) {
            if (!ctx.clientNode()) {
                for (UserOperationFinishFuture f : opFinishFuts.values())
                    f.onNodeLeft(nodeId);
            }

            // Coordinator left
            if (F.eq(coordinator().id(), nodeId)) {
                for (GridFutureAdapter<User> f : authFuts.values())
                    f.onDone(new RetryOnCoordinatorLeftException());

                crdNode = null;

                if (pendingFinishMsg != null)
                    sendFinish(pendingFinishMsg);
            }
        }
    }

    /**
     * Handle finish operation message from a node.
     *
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void onFinishMessage(UUID nodeId, UserManagementOperationFinishedMessage msg) {
        if (log.isDebugEnabled())
            log.debug(msg.toString());

        UserOperationFinishFuture fut = opFinishFuts.get(msg.operationId());

        if (fut == null)
            log.warning("Not found appropriate user operation. [msg=" + msg + ']');
        else {
            synchronized (mux) {
                if (msg.success())
                    fut.onSuccessOnNode(nodeId);
                else
                    fut.onOperationFailOnNode(nodeId, msg.errorMessage());
            }
        }
    }

    /**
     * Called when all required finish messages are received,
     * send ACK message to complete operation futures on all nodes.
     *
     * @param opId Operation ID.
     * @param err Error.
     */
    private void onFinishOperation(IgniteUuid opId, IgniteCheckedException err) {
        try {
            UserAcceptedMessage msg = new UserAcceptedMessage(opId, err);

            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            if (!e.hasCause(IgniteFutureCancelledException.class))
                U.error(log, "Unexpected exception on send UserAcceptedMessage.", e);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void onAuthenticateRequestMessage(UUID nodeId, UserAuthenticateRequestMessage msg) {
        UserAuthenticateResponseMessage respMsg;
        try {
            User u = authenticateOnServer(msg.name(), msg.password());

            respMsg = new UserAuthenticateResponseMessage(msg.id(), u, null);
        }
        catch (IgniteCheckedException e) {
            respMsg = new UserAuthenticateResponseMessage(msg.id(), null, e.toString());

            e.printStackTrace();
        }

        try {
            ctx.io().sendToGridTopic(nodeId, GridTopic.TOPIC_AUTH, respMsg, GridIoPolicy.SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Unexpected exception on send UserAuthenticateResponseMessage.", e);
        }
    }

    /**
     * @param msg Message.
     */
    private void onAuthenticateResponseMessage(UserAuthenticateResponseMessage msg) {
        GridFutureAdapter<User> fut = authFuts.get(msg.id());

        fut.onDone(msg.user(), !msg.success() ? new UserAuthenticationException(msg.errorMessage()) : null);
    }

    /**
     * Local node joined to topology. Discovery cache is available but no discovery custom message are received.
     * Initial user set and initial user operation (received on join) are processed here.
     *
     * @param evt Disco event.
     * @param cache Disco cache.
     * @return Future.
     */
    public IgniteInternalFuture<Boolean> onLocalJoin(DiscoveryEvent evt, DiscoCache cache) {
        synchronized (mux) {
            if (initUsrs != null) {
                exec.execute(new RefreshUsersStorageWorker(initUsrs.usrs));

                for (UserManagementOperation op : initUsrs.activeOps) {
//                    log.info("+++ INIT " + op);

                    UserOperationFinishFuture fut = new UserOperationFinishFuture(op);

                    opFinishFuts.put(op.id(), fut);

                    exec.execute(new UserOperationWorker(op, fut));
                }

                usersInfoVersion = initUsrs.usrVer;
            }
        }

        return null;
    }

    /**
     *
     */
    private final class UserProposedListener implements CustomEventListener<UserProposedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
            final UserProposedMessage msg) {
            if (ctx.isStopping() || ctx.clientNode())
                return;

            synchronized (mux) {
//                log.info("+++ PROP " + msg.operation());


                if (log.isDebugEnabled())
                    log.debug(msg.toString());

                UserManagementOperation op = msg.operation();

                UserOperationFinishFuture fut = new UserOperationFinishFuture(op);
                UserOperationFinishFuture futPrev = opFinishFuts.putIfAbsent(op.id(), fut);

                if (futPrev != null)
                    fut = futPrev;

                activeOperations.add(op);

                exec.execute(new UserOperationWorker(msg.operation(), fut));
            }
        }
    }

    /**
     *
     */
    private final class UserAcceptedListener implements CustomEventListener<UserAcceptedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, UserAcceptedMessage msg) {
            if (log.isDebugEnabled())
                log.debug(msg.toString());

//            log.info("+++ ACK " + msg.operationId());

            UserOperationFinishFuture f = opFinishFuts.get(msg.operationId());

            if (f != null) {
                if (msg.error() != null)
                    f.onDone(null, msg.error());
                else
                    f.onDone();
            }
        }
    }

    /**
     * Future to wait for end of user operation. Removes itself from map when completed.
     */
    private class UserOperationFinishFuture extends GridFutureAdapter<Void> {
        /** */
        private final Collection<UUID> requiredFinish;

        /** */
        private final Collection<UUID> receivedFinish;

        /** User management operation. */
        private final UserManagementOperation op;

        /** Error. */
        private IgniteCheckedException err;

        /**
         * @param op User management operation.
         */
        UserOperationFinishFuture(UserManagementOperation op) {
            this.op = op;

            if (!ctx.clientNode()) {
                requiredFinish = new HashSet<>();
                receivedFinish = new HashSet<>();

                for (ClusterNode node : ctx.discovery().nodes(ctx.discovery().topologyVersionEx())) {
                    if (!node.isClient() && !node.isDaemon())
                        requiredFinish.add(node.id());
                }
            }
            else {
                requiredFinish = null;
                receivedFinish = null;
            }
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
            boolean done = super.onDone(res, err);

            if (done)
                opFinishFuts.remove(op.id(), this);

            return done;
        }

        /**
         * @param nodeId ID of left or failed node.
         */
        synchronized void onNodeLeft(UUID nodeId) {
            assert requiredFinish != null : "Process node left on client";

            requiredFinish.remove(nodeId);

            checkOperationFinished();
        }

        /**
         * @param id Joined node ID.
         */
        synchronized void onNodeJoin(UUID id) {
            assert requiredFinish != null : "Process node join on client";

            requiredFinish.add(id);
        }

        /**
         * @param nodeId Node ID.
         */
        synchronized void onSuccessOnNode(UUID nodeId) {
            assert receivedFinish != null : "Process operation state on client";

            receivedFinish.add(nodeId);

            checkOperationFinished();
        }

        /**
         * @param nodeId Node ID.
         * @param errMsg Error message.
         */
        synchronized void onOperationFailOnNode(UUID nodeId, String errMsg) {
            assert receivedFinish != null : "Process operation state on client";

            if (log.isDebugEnabled())
                log.debug("User operation is failed. [nodeId=" + nodeId + ", err=" + errMsg + ']');

            receivedFinish.add(nodeId);

            UserAuthenticationException e = new UserAuthenticationException("Operation failed. [nodeId=" + nodeId
                + ", op=" + op + ", err=" + errMsg + ']');

            if (err == null)
                err = e;
            else
                err.addSuppressed(e);

            checkOperationFinished();
        }

        /**
         *
         */
        private void checkOperationFinished() {
            if (requiredFinish.equals(receivedFinish))
                onFinishOperation(op.id(), err);
        }
    }

    /**
     * User operation worker.
     */
    private class UserOperationWorker extends GridWorker {
        /** User operation. */
        private final UserManagementOperation op;

        /** Operation future. */
        private final UserOperationFinishFuture fut;

        /**
         * Constructor.
         *
         * @param op Operation.
         * @param fut Operation finish future.
         */
        private UserOperationWorker(UserManagementOperation op, UserOperationFinishFuture fut) {
            super(ctx.igniteInstanceName(), "auth-op-" + op.type(), IgniteAuthenticationProcessor.this.log);

            this.op = op;
            this.fut = fut;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            if (ctx.isStopping())
                return;

            UserManagementOperationFinishedMessage msg0 = null;

            if (sharedCtx != null)
                sharedCtx.database().checkpointReadLock();

            try {
                processOperationLocal(op);

                msg0 = new UserManagementOperationFinishedMessage(op.id(), null);
            }
            catch (Throwable e) {
                log.warning("+++ FAIL " + op, e);

                msg0 = new UserManagementOperationFinishedMessage(op.id(), e.toString());
            }
            finally {
                assert msg0 != null;

                if (sharedCtx != null)
                    sharedCtx.database().checkpointReadUnlock();

                synchronized (mux) {
                    pendingFinishMsg = msg0;

                    sendFinish(pendingFinishMsg);
                }
            }

            try {
                fut.get();

//                log.info("+++ DONE " + op);
            }
            catch (IgniteCheckedException e) {
                if (!e.hasCause(IgniteFutureCancelledException.class))
                    U.error(log, "Unexpected exception on wait for end of user operation.", e);
            }
            finally {
                synchronized (mux) {
                    pendingFinishMsg = null;
                }
            }
        }
    }

    /**
     * @param msg Finish message.
     */
    private void sendFinish(UserManagementOperationFinishedMessage msg) {
        try {
            ctx.io().sendToGridTopic(coordinator(), GridTopic.TOPIC_AUTH, msg, GridIoPolicy.SYSTEM_POOL);
        }
        catch (Exception e) {
            U.error(log, "Failed to send UserManagementOperationFinishedMessage. [op=" + msg.operationId() +
                ", node=" + coordinator() + ", err=" + msg.errorMessage() + ']', e);
        }
    }

    /**
     * Initial users set worker.
     */
    private class RefreshUsersStorageWorker extends GridWorker {
        private final ArrayList<User> newUsrs;

        /**
         * @param usrs New users to store.
         */
        private RefreshUsersStorageWorker(ArrayList<User> usrs) {
            super(ctx.igniteInstanceName(), "refresh-store", IgniteAuthenticationProcessor.this.log);

            newUsrs = usrs;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            if (ctx.clientNode())
                return;

            try {
                Map<String, User> existUsrs = (Map<String, User>)metastorage.readForPredicate(new IgnitePredicate<String>() {
                    @Override public boolean apply(String key) {
                        return key != null && key.startsWith(STORE_USER_PREFIX);
                    }
                });

                for (String name : existUsrs.keySet())
                    metastorage.remove(name);

                for (User u : newUsrs) {
                    metastorage.write(u.name(), u);

                    synchronized (mux) {
                        users.put(u.name(), u);
                    }
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Cannot cleanup old users information at metastorage", e);
            }
        }
    }

    /**
     * Initial data is collected on coordinator to send to join node.
     */
    private static final class InitialUsersData implements Serializable {
        /** Users. */
        private final ArrayList<User> usrs;

        /** Active user operations. */
        private final ArrayList<UserManagementOperation> activeOps;

        /** Users information version. */
        private final long usrVer;

        /**
         * @param usrs Users.
         * @param ops Active operations on cluster.
         * @param ver Users info version.
         */
        InitialUsersData(Collection<User> usrs, List<UserManagementOperation> ops, long ver) {
            this.usrs = new ArrayList<>(usrs);
            activeOps = new ArrayList<>(ops);
            usrVer = ver;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(InitialUsersData.class, this);
        }
    }

    /**
     * Thrown by authenticate futures when coordinator node left
     * to resend authenticate message to the new coordinator.
     */
    private static class RetryOnCoordinatorLeftException extends IgniteCheckedException {
    }
}
