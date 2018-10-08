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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageTree;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.AUTH_PROC;

/**
 *
 */
public class IgniteAuthenticationProcessor extends GridProcessorAdapter implements MetastorageLifecycleListener {
    /** Store user prefix. */
    private static final String STORE_USER_PREFIX = "user.";

    /** Discovery event types. */
    private static final int[] DISCO_EVT_TYPES = new int[] {EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_NODE_JOINED};

    /** User operation finish futures (Operation ID -> future). */
    private final ConcurrentHashMap<IgniteUuid, UserOperationFinishFuture> opFinishFuts = new ConcurrentHashMap<>();

    /** Futures prepared user map. Authentication message ID -> public future. */
    private final ConcurrentMap<IgniteUuid, AuthenticateFuture> authFuts = new ConcurrentHashMap<>();

    /** Whan the future is done the node is ready for authentication. */
    private final GridFutureAdapter<Void> readyForAuthFut = new GridFutureAdapter<>();

    /** Operation mutex. */
    private final Object mux = new Object();

    /** Active operations. Collects to send on joining node. */
    private final Map<IgniteUuid, UserManagementOperation> activeOps = Collections.synchronizedMap(new LinkedHashMap<>());

    /** User map. */
    private ConcurrentMap<String, User> users;

    /** Shared context. */
    @GridToStringExclude
    private GridCacheSharedContext<?, ?> sharedCtx;

    /** Meta storage. */
    private ReadWriteMetastorage metastorage;

    /** Executor. */
    private IgniteThreadPoolExecutor exec;

    /** Coordinator node. */
    private ClusterNode crdNode;

    /** Is authentication enabled. */
    private boolean isEnabled;

    /** Disconnected flag. */
    private volatile boolean disconnected;

    /** Finish message of the current operation. May be resend when coordinator node leave. */
    private UserManagementOperationFinishedMessage curOpFinishMsg;

    /** Initial users map and operations received from coordinator on the node joined to the cluster. */
    private InitialUsersData initUsrs;

    /** I/O message listener. */
    private GridMessageListener ioLsnr;

    /** System discovery message listener. */
    private DiscoveryEventListener discoLsnr;

    /** Node activate future. */
    private final GridFutureAdapter<Void> activateFut = new GridFutureAdapter<>();

    /** Validate error. */
    private String validateErr;

    /**
     * @param ctx Kernal context.
     */
    public IgniteAuthenticationProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        isEnabled = ctx.config().isAuthenticationEnabled();

        if (isEnabled && !GridCacheUtils.isPersistenceEnabled(ctx.config())) {
            isEnabled = false;

            throw new IgniteCheckedException("Authentication can be enabled only for cluster with enabled persistence."
                + " Check the DataRegionConfiguration");
        }

        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);

        ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_AUTHENTICATION_ENABLED, isEnabled);

        GridDiscoveryManager discoMgr = ctx.discovery();

        GridIoManager ioMgr = ctx.io();

        discoMgr.setCustomEventListener(UserProposedMessage.class, new UserProposedListener());

        discoMgr.setCustomEventListener(UserAcceptedMessage.class, new UserAcceptedListener());

        discoLsnr = (evt, discoCache) -> {
            if (!isEnabled || ctx.isStopping())
                return;

            switch (evt.type()) {
                case EVT_NODE_LEFT:
                case EVT_NODE_FAILED:
                    onNodeLeft(evt.eventNode().id());
                    break;

                case EVT_NODE_JOINED:
                    onNodeJoin(evt.eventNode());
                    break;
            }
        };

        ctx.event().addDiscoveryEventListener(discoLsnr, DISCO_EVT_TYPES);

        ioLsnr = (nodeId, msg, plc) -> {
            if (!isEnabled || ctx.isStopping())
                return;

            if (msg instanceof UserManagementOperationFinishedMessage)
                onFinishMessage(nodeId, (UserManagementOperationFinishedMessage)msg);
            else if (msg instanceof UserAuthenticateRequestMessage)
                onAuthenticateRequestMessage(nodeId, (UserAuthenticateRequestMessage)msg);
            else if (msg instanceof UserAuthenticateResponseMessage)
                onAuthenticateResponseMessage((UserAuthenticateResponseMessage)msg);
        };

        ioMgr.addMessageListener(GridTopic.TOPIC_AUTH, ioLsnr);

        exec = new IgniteThreadPoolExecutor(
            "auth",
            ctx.config().getIgniteInstanceName(),
            1,
            1,
            0,
            new LinkedBlockingQueue<>());
    }

    /**
     * On cache processor started.
     */
    public void cacheProcessorStarted() {
        sharedCtx = ctx.cache().context();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (!isEnabled)
            return;

        ctx.io().removeMessageListener(GridTopic.TOPIC_AUTH, ioLsnr);

        ctx.event().removeDiscoveryEventListener(discoLsnr, DISCO_EVT_TYPES);

        cancelFutures("Node stopped");

        if (!cancel)
            exec.shutdown();
        else
            exec.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (!isEnabled)
            return;

        synchronized (mux) {
            cancelFutures("Kernal stopped.");
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        super.onKernalStart(active);

        if (validateErr != null)
            throw new IgniteCheckedException(validateErr);
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        if (!isEnabled)
            return;

        synchronized (mux) {
            assert !disconnected;

            disconnected = true;

            cancelFutures("Client node was disconnected from topology (operation result is unknown).");
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean active) {
        if (!isEnabled)
            return null;

        synchronized (mux) {
            assert disconnected;

            disconnected = false;

            return null;
        }
    }

    /**
     * Authenticate user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @return User object on successful authenticate. Otherwise returns {@code null}.
     * @throws IgniteCheckedException On error.
     * @throws IgniteAccessControlException On authentication error.
     */
    public AuthorizationContext authenticate(String login, String passwd) throws IgniteCheckedException {
        checkEnabled();

        if (F.isEmpty(login))
            throw new IgniteAccessControlException("The user name or password is incorrect [userName=" + login + ']');

        if (ctx.clientNode()) {
            while (true) {
                AuthenticateFuture fut;

                synchronized (mux) {
                    ClusterNode rndNode = U.randomServerNode(ctx);

                    fut = new AuthenticateFuture(rndNode.id());

                    UserAuthenticateRequestMessage msg = new UserAuthenticateRequestMessage(login, passwd);

                    authFuts.put(msg.id(), fut);

                    ctx.io().sendToGridTopic(rndNode, GridTopic.TOPIC_AUTH, msg, GridIoPolicy.SYSTEM_POOL);
                }

                fut.get();

                if (fut.retry())
                    continue;

                return new AuthorizationContext(User.create(login));
            }
        }
        else
            return new AuthorizationContext(authenticateOnServer(login, passwd));
    }

    /**
     * @param login User's login.
     * @param passwd Password.
     * @throws UserManagementException On error.
     */
    public static void validate(String login, String passwd) throws UserManagementException {
        if (F.isEmpty(login))
            throw new UserManagementException("User name is empty");

        if (F.isEmpty(passwd))
            throw new UserManagementException("Password is empty");

        if ((STORE_USER_PREFIX + login).getBytes().length > MetastorageTree.MAX_KEY_LEN)
            throw new UserManagementException("User name is too long. " +
                "The user name length must be less then 60 bytes in UTF8");
    }

    /**
     * Adds new user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @throws IgniteCheckedException On error.
     */
    public void addUser(String login, String passwd) throws IgniteCheckedException {
        validate(login, passwd);

        UserManagementOperation op = new UserManagementOperation(User.create(login, passwd),
            UserManagementOperation.OperationType.ADD);

        execUserOperation(op).get();
    }

    /**
     * @param login User name.
     * @throws IgniteCheckedException On error.
     */
    public void removeUser(String login) throws IgniteCheckedException {
        UserManagementOperation op = new UserManagementOperation(User.create(login),
            UserManagementOperation.OperationType.REMOVE);

        execUserOperation(op).get();
    }

    /**
     * @param login User name.
     * @param passwd User password.
     * @throws IgniteCheckedException On error.
     */
    public void updateUser(String login, String passwd) throws IgniteCheckedException {
        UserManagementOperation op = new UserManagementOperation(User.create(login, passwd),
            UserManagementOperation.OperationType.UPDATE);

        execUserOperation(op).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        if (!ctx.clientNode()) {
            users = new ConcurrentHashMap<>();

            Map<String, User> readUsers = (Map<String, User>)metastorage.readForPredicate(
                (IgnitePredicate<String>)key -> key != null && key.startsWith(STORE_USER_PREFIX));

            for (User u : readUsers.values())
                users.put(u.name(), u);
        }
        else
            users = null;
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) {
        if (!ctx.clientNode())
            this.metastorage = metastorage;
        else
            this.metastorage = null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return DiscoveryDataExchangeType.AUTH_PROC;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        Boolean rmtEnabled = node.attribute(IgniteNodeAttributes.ATTR_AUTHENTICATION_ENABLED);

        if (isEnabled && rmtEnabled == null) {
            String errMsg = "Failed to add node to topology because user authentication is enabled on cluster and " +
                "the node doesn't support user authentication [nodeId=" + node.id() + ']';

            return new IgniteNodeValidationResult(node.id(), errMsg, errMsg);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        // 1. Collect users info only on coordinator
        // 2. Doesn't collect users info to send on client node due to security reason.
        if (!isEnabled || !isLocalNodeCoordinator() || dataBag.isJoiningNodeClient())
            return;

        synchronized (mux) {
            if (!dataBag.commonDataCollectedFor(AUTH_PROC.ordinal())) {
                InitialUsersData d = new InitialUsersData(users.values(), activeOps.values());

                if (log.isDebugEnabled())
                    log.debug("Collected initial users data: " + d);

                dataBag.addGridCommonData(AUTH_PROC.ordinal(), d);
            }
        }
    }

    /**
     * Checks whether local node is coordinator. Nodes that are leaving or failed
     * (but are still in topology) are removed from search.
     *
     * @return {@code true} if local node is coordinator.
     */
    private boolean isLocalNodeCoordinator() {
        DiscoverySpi spi = ctx.discovery().getInjectedDiscoverySpi();

        if (spi instanceof TcpDiscoverySpi)
            return ((TcpDiscoverySpi)spi).isLocalNodeCoordinator();
        else
            return F.eq(ctx.localNodeId(), coordinator().id());
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        initUsrs = (InitialUsersData)data.commonData();
    }

    /**
     * @return {@code true} if authentication is enabled, {@code false} if not.
     */
    public boolean enabled() {
        return isEnabled;
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
     *
     */
    private void checkEnabled() {
        if (!isEnabled) {
            throw new IgniteException("Can not perform the operation because the authentication" +
                " is not enabled for the cluster.");
        }
    }

    /**
     */
    private void addDefaultUser() {
        assert users != null && users.isEmpty();

        User dfltUser = User.defaultUser();

        // Put to local map to be ready for authentication.
        users.put(dfltUser.name(), dfltUser);

        // Put to MetaStore when it will be ready.
        exec.execute(new RefreshUsersStorageWorker(new ArrayList<>(Collections.singleton(dfltUser))));
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

        readyForAuthFut.get();

        User usr;

        usr = users.get(login);

        if (usr == null)
            throw new IgniteAccessControlException("The user name or password is incorrect [userName=" + login + ']');

        if (usr.authorize(passwd))
            return usr;
        else
            throw new IgniteAccessControlException("The user name or password is incorrect [userName=" + login + ']');
    }

    /**
     * @param op User operation.
     * @return Operation future.
     * @throws IgniteCheckedException On error.
     */
    private UserOperationFinishFuture execUserOperation(UserManagementOperation op) throws IgniteCheckedException {
        checkActivate();
        checkEnabled();

        synchronized (mux) {
            if (disconnected) {
                throw new UserManagementException("Failed to initiate user management operation because "
                    + "client node is disconnected.");
            }

            AuthorizationContext actx = AuthorizationContext.context();

            if (actx == null)
                throw new IgniteAccessControlException("Operation not allowed: authorized context is empty.");

            actx.checkUserOperation(op);

            UserOperationFinishFuture fut = new UserOperationFinishFuture(op.id());

            opFinishFuts.put(op.id(), fut);

            UserProposedMessage msg = new UserProposedMessage(op);

            ctx.discovery().sendCustomEvent(msg);

            return fut;
        }
    }

    /**
     * @param op The operation with users.
     * @throws IgniteCheckedException On error.
     */
    private void processOperationLocal(UserManagementOperation op) throws IgniteCheckedException {
        assert op != null && op.user() != null : "Invalid operation: " + op;

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
     * @throws IgniteCheckedException On error.
     */
    private void addUserLocal(final UserManagementOperation op) throws IgniteCheckedException {
        User usr = op.user();

        String userName = usr.name();

        if (users.containsKey(userName))
            throw new UserManagementException("User already exists [login=" + userName + ']');

        metastorage.write(STORE_USER_PREFIX + userName, usr);

        synchronized (mux) {
            activeOps.remove(op.id());

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

        if (!users.containsKey(usr.name()))
            throw new UserManagementException("User doesn't exist [userName=" + usr.name() + ']');

        metastorage.remove(STORE_USER_PREFIX + usr.name());

        synchronized (mux) {
            activeOps.remove(op.id());

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

        if (!users.containsKey(usr.name()))
            throw new UserManagementException("User doesn't exist [userName=" + usr.name() + ']');

        metastorage.write(STORE_USER_PREFIX + usr.name(), usr);

        synchronized (mux) {
            activeOps.remove(op.id());

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

                if (res == null
                    && !ctx.discovery().allNodes().isEmpty()
                    && ctx.discovery().aliveServerNodes().isEmpty()) {
                    U.warn(log, "Cannot find the server coordinator node. "
                        + "Possible a client is started with forceServerMode=true. " +
                        "Security warning: user authentication will be disabled on the client.");

                    isEnabled = false;
                }
                else
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
        synchronized (mux) {
            for (UserOperationFinishFuture fut : opFinishFuts.values())
                fut.onDone(null, new IgniteFutureCancelledException(msg));
        }

        for (GridFutureAdapter<Void> fut : authFuts.values())
            fut.onDone(null, new IgniteFutureCancelledException(msg));
    }

    /**
     * @param node Joined node ID.
     */
    private void onNodeJoin(ClusterNode node) {
        if (isNodeHoldsUsers(ctx.discovery().localNode()) && isNodeHoldsUsers(node)) {
            synchronized (mux) {
                for (UserOperationFinishFuture f : opFinishFuts.values())
                    f.onNodeJoin(node.id());
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

            // Found all auth requests that were be sent to left node.
            // Complete these futures with special exception to retry authentication.
            for (Iterator<Map.Entry<IgniteUuid, AuthenticateFuture>> it = authFuts.entrySet().iterator();
                it.hasNext(); ) {
                AuthenticateFuture f = it.next().getValue();

                if (F.eq(nodeId, f.nodeId())) {
                    f.retry(true);

                    f.onDone();

                    it.remove();
                }
            }

            // Coordinator left
            if (F.eq(coordinator().id(), nodeId)) {
                // Refresh info about coordinator node.
                crdNode = null;

                if (curOpFinishMsg != null)
                    sendFinish(curOpFinishMsg);
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

        synchronized (mux) {
            UserOperationFinishFuture fut = opFinishFuts.get(msg.operationId());

            if (fut == null) {
                fut = new UserOperationFinishFuture(msg.operationId());

                opFinishFuts.put(msg.operationId(), fut);
            }

            if (msg.success())
                fut.onSuccessOnNode(nodeId);
            else
                fut.onOperationFailOnNode(nodeId, msg.errorMessage());
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

            respMsg = new UserAuthenticateResponseMessage(msg.id(), null);
        }
        catch (IgniteCheckedException e) {
            respMsg = new UserAuthenticateResponseMessage(msg.id(), e.toString());

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
        GridFutureAdapter<Void> fut = authFuts.get(msg.id());

        fut.onDone(null, !msg.success() ? new IgniteAccessControlException(msg.errorMessage()) : null);

        authFuts.remove(msg.id());
    }

    /**
     * Local node joined to topology. Discovery cache is available but no discovery custom message are received.
     * Initial user set and initial user operation (received on join) are processed here.
     */
    public void onLocalJoin() {
        if (coordinator() == null)
            return;

        if (F.eq(coordinator().id(), ctx.localNodeId())) {
            if (!isEnabled)
                return;

            assert initUsrs == null;

            // Creates default user on coordinator if it is the first start of PDS cluster
            // or start of in-memory cluster.
            if (users.isEmpty())
                addDefaultUser();
        }
        else {
            Boolean rmtEnabled = coordinator().attribute(IgniteNodeAttributes.ATTR_AUTHENTICATION_ENABLED);

            // The cluster doesn't support authentication (ver < 2.5)
            if (rmtEnabled == null)
                rmtEnabled = false;

            if (isEnabled != rmtEnabled) {
                if (rmtEnabled)
                    U.warn(log, "User authentication is enabled on cluster. Enables on local node");
                else {
                    validateErr = "User authentication is disabled on cluster";

                    return;
                }
            }

            isEnabled = rmtEnabled;

            if (!isEnabled) {
                try {
                    stop(false);
                }
                catch (IgniteCheckedException e) {
                    U.warn(log, "Unexpected exception on stopped authentication processor", e);
                }

                return;
            }

            if (ctx.clientNode())
                return;

            assert initUsrs != null;

            // Can be empty on initial start of PDS cluster (default user will be created and stored after activate)
            if (!F.isEmpty(initUsrs.usrs)) {
                if (users == null)
                    users = new ConcurrentHashMap<>();
                else
                    users.clear();

                for (User u : initUsrs.usrs)
                    users.put(u.name(), u);

                exec.execute(new RefreshUsersStorageWorker(initUsrs.usrs));
            }

            for (UserManagementOperation op : initUsrs.activeOps)
                submitOperation(op);
        }

        readyForAuthFut.onDone();
    }

    /**
     * Called on node activate.
     */
    public void onActivate() {
        activateFut.onDone();
    }

    /**
     *
     */
    private void waitActivate() {
        try {
            activateFut.get();
        }
        catch (IgniteCheckedException e) {
            // No-op.
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
            U.error(log, "Failed to send UserManagementOperationFinishedMessage [op=" + msg.operationId() +
                ", node=" + coordinator() + ", err=" + msg.errorMessage() + ']', e);
        }
    }

    /**
     * Register operation, future and add operation worker to execute queue.
     *
     * @param op User operation.
     */
    private void submitOperation(UserManagementOperation op) {
        synchronized (mux) {
            UserOperationFinishFuture fut = opFinishFuts.get(op.id());

            if (fut == null) {
                fut = new UserOperationFinishFuture(op.id());

                opFinishFuts.put(op.id(), fut);
            }

            if (!fut.workerSubmitted()) {
                fut.workerSubmitted(true);

                activeOps.put(op.id(), op);

                exec.execute(new UserOperationWorker(op, fut));
            }
        }
    }

    /**
     * @param n Node.
     * @return {@code true} if node holds user information. Otherwise returns {@code false}.
     */
    private static boolean isNodeHoldsUsers(ClusterNode n) {
        return !n.isClient() && !n.isDaemon();
    }

    /**
     * Initial data is collected on coordinator to send to join node.
     */
    private static final class InitialUsersData implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Users. */
        @GridToStringInclude
        private final ArrayList<User> usrs;

        /** Active user operations. */
        @GridToStringInclude
        private final ArrayList<UserManagementOperation> activeOps;

        /**
         * @param usrs Users.
         * @param ops Active operations on cluster.
         */
        InitialUsersData(Collection<User> usrs, Collection<UserManagementOperation> ops) {
            this.usrs = new ArrayList<>(usrs);
            activeOps = new ArrayList<>(ops);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(InitialUsersData.class, this);
        }
    }

    /**i
     *
     */
    private final class UserProposedListener implements CustomEventListener<UserProposedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
            final UserProposedMessage msg) {
            if (!isEnabled || ctx.isStopping() || ctx.clientNode())
                return;

            if (log.isDebugEnabled())
                log.debug(msg.toString());

            submitOperation(msg.operation());
        }
    }

    /**
     *
     */
    private final class UserAcceptedListener implements CustomEventListener<UserAcceptedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, UserAcceptedMessage msg) {
            if (!isEnabled || ctx.isStopping())
                return;

            if (log.isDebugEnabled())
                log.debug(msg.toString());

            synchronized (mux) {
                UserOperationFinishFuture f = opFinishFuts.get(msg.operationId());

                if (f != null) {
                    if (msg.error() != null)
                        f.onDone(null, msg.error());
                    else
                        f.onDone();
                }
            }
        }
    }

    /**
     * Future to wait for end of user operation. Removes itself from map when completed.
     */
    private class UserOperationFinishFuture extends GridFutureAdapter<Void> {
        /** */
        private final Set<UUID> requiredFinish;

        /** */
        private final Set<UUID> receivedFinish;

        /** User management operation ID. */
        private final IgniteUuid opId;

        /** Worker has been already submitted flag. */
        private boolean workerSubmitted;

        /** Error. */
        private IgniteCheckedException err;


        /**
         * @param opId User management operation ID.
         */
        UserOperationFinishFuture(IgniteUuid opId) {
            this.opId = opId;

            if (!ctx.clientNode()) {
                requiredFinish = new HashSet<>();
                receivedFinish = new HashSet<>();

                for (ClusterNode node : ctx.discovery().nodes(ctx.discovery().topologyVersionEx())) {
                    if (isNodeHoldsUsers(node))
                        requiredFinish.add(node.id());
                }
            }
            else {
                requiredFinish = null;
                receivedFinish = null;
            }
        }

        /**
         * @return Worker submitted flag.
         */
        boolean workerSubmitted() {
            return workerSubmitted;
        }

        /**
         * @param val Worker submitted flag.
         */
        void workerSubmitted(boolean val) {
            workerSubmitted = val;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
            boolean done = super.onDone(res, err);

            synchronized (mux) {
                if (done)
                    opFinishFuts.remove(opId, this);
            }

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
                log.debug("User operation is failed [nodeId=" + nodeId + ", err=" + errMsg + ']');

            receivedFinish.add(nodeId);

            UserManagementException e = new UserManagementException("Operation failed [nodeId=" + nodeId
                + ", opId=" + opId + ", err=" + errMsg + ']');

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
            if (receivedFinish.containsAll(requiredFinish))
                onFinishOperation(opId, err);
        }
    }

    /**
     *
     */
    private static class AuthenticateFuture extends GridFutureAdapter<Void> {
        /** Node id. */
        private final UUID nodeId;

        /** Node id. */
        private boolean retry;

        /**
         * @param nodeId ID of the node that processes authentication request.
         */
       AuthenticateFuture(UUID nodeId) {
           this.nodeId = nodeId;
       }

        /**
         * @return ID of the node that processes authentication request.
         */
        UUID nodeId() {
            return nodeId;
        }

        /**
         * @return {@code true} if need retry (aftyer node left).
         */
        boolean retry() {
            return retry;
        }

        /**
         * @param retry Set retry flag.
         */
        void retry(boolean retry) {
            this.retry = retry;
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

            waitActivate();

            UserManagementOperationFinishedMessage msg0;

            if (sharedCtx != null)
                sharedCtx.database().checkpointReadLock();

            try {
                processOperationLocal(op);

                msg0 = new UserManagementOperationFinishedMessage(op.id(), null);
            }
            catch (UserManagementException e) {
                msg0 = new UserManagementOperationFinishedMessage(op.id(), e.toString());

                // Remove failed operation from active operations.
                activeOps.remove(op.id());
            }
            catch (Throwable e) {
                log.warning("Unexpected exception on perform user management operation", e);

                msg0 = new UserManagementOperationFinishedMessage(op.id(), e.toString());

                // Remove failed operation from active operations.
                activeOps.remove(op.id());
            }
            finally {
                if (sharedCtx != null)
                    sharedCtx.database().checkpointReadUnlock();
            }

            curOpFinishMsg = msg0;

            sendFinish(curOpFinishMsg);

            try {
                fut.get();
            }
            catch (IgniteCheckedException e) {
                if (!e.hasCause(IgniteFutureCancelledException.class))
                    U.error(log, "Unexpected exception on wait for end of user operation.", e);
            }
            finally {
                curOpFinishMsg = null;
            }
        }
    }

    /**
     * Initial users set worker.
     */
    private class RefreshUsersStorageWorker extends GridWorker {
        /** */
        private final ArrayList<User> newUsrs;

        /**
         * @param usrs New users to store.
         */
        private RefreshUsersStorageWorker(ArrayList<User> usrs) {
            super(ctx.igniteInstanceName(), "refresh-store", IgniteAuthenticationProcessor.this.log);

            assert !F.isEmpty(usrs);

            newUsrs = usrs;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            if (ctx.clientNode())
                return;

            waitActivate();

            if (sharedCtx != null)
                sharedCtx.database().checkpointReadLock();

            try {
                Map<String, User> existUsrs = (Map<String, User>)metastorage.readForPredicate(
                    (IgnitePredicate<String>)key -> key != null && key.startsWith(STORE_USER_PREFIX));

                for (String key : existUsrs.keySet())
                    metastorage.remove(key);

                for (User u : newUsrs)
                    metastorage.write(STORE_USER_PREFIX + u.name(), u);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Cannot cleanup old users information at metastorage", e);
            }
            finally {
                if (sharedCtx != null)
                    sharedCtx.database().checkpointReadUnlock();
            }
        }
    }
}
