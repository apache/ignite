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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageTree;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.IgniteSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecuritySubjectType;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.AUTH_PROC;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_AUTHENTICATION_ENABLED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.internal.processors.authentication.UserManagementOperation.OperationType.ADD;
import static org.apache.ignite.internal.processors.authentication.UserManagementOperation.OperationType.REMOVE;
import static org.apache.ignite.internal.processors.authentication.UserManagementOperation.OperationType.UPDATE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;
import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_NODE;

/**
 *
 */
public class IgniteAuthenticationProcessor extends GridProcessorAdapter implements GridSecurityProcessor,
    MetastorageLifecycleListener, PartitionsExchangeAware {
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
    private ConcurrentMap<UUID, User> users;

    /** Shared context. */
    @GridToStringExclude
    private GridCacheSharedContext<?, ?> sharedCtx;

    /** Meta storage. */
    private ReadWriteMetastorage metastorage;

    /** Executor. */
    private IgniteThreadPoolExecutor exec;

    /** Coordinator node. */
    private ClusterNode crdNode;

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

    /**
     * @param ctx Kernal context.
     */
    public IgniteAuthenticationProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** Starts processor. */
    public void startProcessor() throws IgniteCheckedException {
        if (!GridCacheUtils.isPersistenceEnabled(ctx.config())) {
            throw new IgniteCheckedException("Authentication can be enabled only for cluster with enabled persistence."
                + " Check the DataRegionConfiguration");
        }

        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);

        ctx.addNodeAttribute(ATTR_AUTHENTICATION_ENABLED, true);

        sharedCtx = ctx.cache().context();

        sharedCtx.exchange().registerExchangeAwareComponent(this);

        GridDiscoveryManager discoMgr = ctx.discovery();

        GridIoManager ioMgr = ctx.io();

        discoMgr.setCustomEventListener(UserProposedMessage.class, new UserProposedListener());

        discoMgr.setCustomEventListener(UserAcceptedMessage.class, new UserAcceptedListener());

        discoMgr.localJoinFuture().listen(fut -> onLocalJoin());

        discoLsnr = (evt, discoCache) -> {
            if (ctx.isStopping())
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
            if (ctx.isStopping())
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

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (ioLsnr != null)
            ctx.io().removeMessageListener(GridTopic.TOPIC_AUTH, ioLsnr);

        if (discoLsnr != null)
            ctx.event().removeDiscoveryEventListener(discoLsnr, DISCO_EVT_TYPES);

        cancelFutures("Node stopped");

        if (exec != null) {
            if (!cancel)
                exec.shutdown();
            else
                exec.shutdownNow();
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        synchronized (mux) {
            cancelFutures("Kernal stopped.");
        }
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

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext authCtx) throws IgniteCheckedException {
        SecurityCredentials creds = authCtx.credentials();

        String login = (String)creds.getLogin();

        if (F.isEmpty(login))
            throw new IgniteAccessControlException("The user name or password is incorrect [userName=" + login + ']');

        String passwd = (String)creds.getPassword();

        UUID subjId;

        if (ctx.clientNode()) {
            if (ctx.discovery().aliveServerNodes().isEmpty()) {
                throw new IgniteAccessControlException("No alive server node was found to which the authentication" +
                    " operation could be delegated. It is possible that the client node has been started with the" +
                    " \"forceServerMode\" flag enabled and no server node had been started yet.");
            }

            AuthenticateFuture fut;

            do {
                synchronized (mux) {
                    ClusterNode rndNode = U.randomServerNode(ctx);

                    fut = new AuthenticateFuture(rndNode.id());

                    UserAuthenticateRequestMessage msg = new UserAuthenticateRequestMessage(login, passwd);

                    authFuts.put(msg.id(), fut);

                    ctx.io().sendToGridTopic(rndNode, GridTopic.TOPIC_AUTH, msg, GridIoPolicy.SYSTEM_POOL);
                }

                fut.get();
            } while (fut.retry());

            subjId = toSubjectId(login);
        }
        else
            subjId = authenticateOnServer(login, passwd);

        return new SecurityContextImpl(subjId, login, authCtx.subjectType(), authCtx.address());
    }

    /**
     * @param login User's login.
     * @param passwd Password.
     * @throws UserManagementException On error.
     */
    public static void validate(String login, char[] passwd) throws UserManagementException {
        if (F.isEmpty(login))
            throw new UserManagementException("User name is empty");

        if (F.isEmpty(passwd))
            throw new UserManagementException("Password is empty");

        if ((STORE_USER_PREFIX + login).getBytes().length > MetastorageTree.MAX_KEY_LEN)
            throw new UserManagementException("User name is too long. " +
                "The user name length must be less then 60 bytes in UTF8");
    }

    /** {@inheritDoc} */
    @Override public void createUser(String login, char[] passwd) throws IgniteCheckedException {
        validate(login, passwd);

        UserManagementOperation op = new UserManagementOperation(User.create(login, new String(passwd)), ADD);

        execUserOperation(op).get();
    }

    /** {@inheritDoc} */
    @Override public void dropUser(String login) throws IgniteCheckedException {
        UserManagementOperation op = new UserManagementOperation(User.create(login), REMOVE);

        execUserOperation(op).get();
    }

    /** {@inheritDoc} */
    @Override public void alterUser(String login, char[] passwd) throws IgniteCheckedException {
        UserManagementOperation op = new UserManagementOperation(User.create(login, new String(passwd)), UPDATE);

        execUserOperation(op).get();
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        if (!ctx.clientNode()) {
            users = new ConcurrentHashMap<>();

            metastorage.iterate(STORE_USER_PREFIX, (key, val) -> {
                User u = (User)val;

                User cur = users.putIfAbsent(toSubjectId(u.name()), u);

                if (cur != null) {
                    throw new IllegalStateException("Security users with conflicting IDs were found while reading from" +
                        " metastorage [logins=" + u.name() + ", " + cur.name() + "]. It is possible that the Ignite" +
                        " metastorage is corrupted or the specified users were created bypassing the Ignite Security API.");
                }
            }, true);
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
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        // 1. Collect users info only on coordinator
        // 2. Doesn't collect users info to send on client node due to security reason.
        if (!isLocalNodeCoordinator() || dataBag.isJoiningNodeClient())
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

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
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
     */
    private void addDefaultUser() {
        assert users != null && users.isEmpty();

        User dfltUser = User.defaultUser();

        // Put to local map to be ready for authentication.
        users.put(toSubjectId(dfltUser.name()), dfltUser);

        // Put to MetaStore when it will be ready.
        exec.execute(new RefreshUsersStorageWorker(new ArrayList<>(Collections.singleton(dfltUser))));
    }

    /**
     * Authenticate user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @return Authenticated user security ID.
     * @throws IgniteCheckedException On authentication error.
     */
    private UUID authenticateOnServer(String login, String passwd) throws IgniteCheckedException {
        assert !ctx.clientNode() : "Must be used on server node";

        readyForAuthFut.get();

        UUID subjId = toSubjectId(login);

        User usr = findUser(subjId, login);

        if (usr == null || !usr.authorize(passwd))
            throw new IgniteAccessControlException("The user name or password is incorrect [userName=" + login + ']');

        return subjId;
    }

    /**
     * @param op User operation.
     * @return Operation future.
     * @throws IgniteCheckedException On error.
     */
    private UserOperationFinishFuture execUserOperation(UserManagementOperation op) throws IgniteCheckedException {
        checkActivate();

        synchronized (mux) {
            if (disconnected) {
                throw new UserManagementException("Failed to initiate user management operation because "
                    + "client node is disconnected.");
            }

            checkUserOperation(op);

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

        UUID subjId = toSubjectId(userName);

        if (users.get(subjId) != null)
            throw new UserManagementException("User already exists [login=" + userName + ']');

        metastorage.write(STORE_USER_PREFIX + userName, usr);

        synchronized (mux) {
            activeOps.remove(op.id());

            users.put(subjId, usr);
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

        String login = usr.name();

        UUID subjId = toSubjectId(login);

        if (findUser(subjId, login) == null)
            throw new UserManagementException("User doesn't exist [userName=" + login + ']');

        metastorage.remove(STORE_USER_PREFIX + login);

        synchronized (mux) {
            activeOps.remove(op.id());

            users.remove(subjId);
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

        String login = usr.name();

        UUID subjId = toSubjectId(login);

        if (findUser(subjId, login) == null)
            throw new UserManagementException("User doesn't exist [userName=" + login + ']');

        metastorage.write(STORE_USER_PREFIX + login, usr);

        synchronized (mux) {
            activeOps.remove(op.id());

            users.put(subjId, usr);
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
                        + "Possible a client is started with forceServerMode=true.");
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
            authenticateOnServer(msg.name(), msg.password());

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
    private void onLocalJoin() {
        if (ctx.isDaemon() || ctx.clientDisconnected() || coordinator() == null)
            return;

        if (F.eq(coordinator().id(), ctx.localNodeId())) {
            assert initUsrs == null;

            // Creates default user on coordinator if it is the first start of PDS cluster
            // or start of in-memory cluster.
            if (users.isEmpty())
                addDefaultUser();
        }
        else {
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
                    users.put(toSubjectId(u.name()), u);

                exec.execute(new RefreshUsersStorageWorker(initUsrs.usrs));
            }

            for (UserManagementOperation op : initUsrs.activeOps)
                submitOperation(op);
        }

        readyForAuthFut.onDone();
    }

    /** {@inheritDoc} */
    @Override public void onDoneBeforeTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
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
     * {@inheritDoc}
     *
     * The current implementation of {@link GridSecurityProcessor} allows any Ignite node to join the Ignite cluster
     * without authentication check.
     */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) throws IgniteCheckedException {
        return new SecurityContextImpl(
            node.id(),
            node.attribute(ATTR_IGNITE_INSTANCE_NAME),
            REMOTE_NODE,
            new InetSocketAddress(F.first(node.addresses()), 0));
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm, SecurityContext securityCtx) throws SecurityException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        // No-op.
    }

    /**
     * {@inheritDoc}
     *
     * This method works with the assumption that {@link SecurityContext} associated with the Ignite node is stored in
     * node attributes and is obtained automatically by the Ignite using the node ID
     * (see {@link IgniteSecurityProcessor#withContext(java.util.UUID)}). Since we use the node ID as the subject ID
     * during node authentication, this method is used for obtaining security context for thin clients only.
     * Note, that the returned security context does not contain the address of the security subject.
     * Since the client node does not store user data, the {@link SecurityContext} returned by the client node does
     * not contain any user information, address, or username.
     */
    @Override public SecurityContext securityContext(UUID subjId) {
        if (ctx.clientNode())
            return new SecurityContextImpl(subjId, null, REMOTE_CLIENT, null);

        User user = users.get(subjId);

        return user == null ? null : new SecurityContextImpl(subjId, user.name(), REMOTE_CLIENT, null);
    }

    /**
     * Gets the user with the specified ID and login. It is necessary to check the login to make sure that there was
     * no collision when calculating the user ID.
     */
    private User findUser(UUID subjId, String login) {
        User user = users.get(subjId);

        if (user == null || !user.name().equals(login))
            return null;

        return user;
    }

    /** Calculates user id based on specified login. */
    private UUID toSubjectId(String login) {
        return UUID.nameUUIDFromBytes(login.getBytes());
    }

    /**
     * @param op User operation to check.
     * @throws IgniteAccessControlException If operation check fails: user hasn't permissions for user management
     *      or try to remove default user.
     */
    public void checkUserOperation(UserManagementOperation op) throws IgniteAccessControlException {
        assert op != null;

        SecuritySubject subj = ctx.security().securityContext().subject();

        if (subj.type() == REMOTE_NODE) {
            throw new IgniteAccessControlException("User management operations initiated on behalf of" +
                " the Ignite node are not expected.");
        }

        if (!User.DFAULT_USER_NAME.equals(subj.login())
            && !(UserManagementOperation.OperationType.UPDATE == op.type() && subj.login().equals(op.user().name())))
            throw new IgniteAccessControlException("User management operations are not allowed for user. " +
                "[curUser=" + subj.login() + ']');

        if (op.type() == UserManagementOperation.OperationType.REMOVE
            && User.DFAULT_USER_NAME.equals(op.user().name()))
            throw new IgniteAccessControlException("Default user cannot be removed.");
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
            if (ctx.isStopping() || ctx.clientNode())
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
            if (ctx.isStopping())
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
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            if (ctx.clientNode())
                return;

            waitActivate();

            if (sharedCtx != null)
                sharedCtx.database().checkpointReadLock();

            try {
                Set<String> existUsrsKeys = new HashSet<>();

                metastorage.iterate(STORE_USER_PREFIX, (key, val) -> existUsrsKeys.add(key), false);

                for (String key : existUsrsKeys)
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

    /** Represents {@link SecuritySubject} implementation. */
    private static class SecuritySubjectImpl implements SecuritySubject {
        /** */
        private static final long serialVersionUID = 0L;

        /** Security subject identifier. */
        private final UUID id;

        /** Security subject login.  */
        private final String login;

        /** Security subject type. */
        private final SecuritySubjectType type;

        /** Security subject address. */
        private final InetSocketAddress addr;

        /** */
        public SecuritySubjectImpl(UUID id, String login, SecuritySubjectType type, InetSocketAddress addr) {
            this.id = id;
            this.login = login;
            this.type = type;
            this.addr = addr;
        }

        /** {@inheritDoc} */
        @Override public UUID id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String login() {
            return login;
        }

        /** {@inheritDoc} */
        @Override public SecuritySubjectType type() {
            return type;
        }

        /** {@inheritDoc} */
        @Override public InetSocketAddress address() {
            return addr;
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet permissions() {
            return ALLOW_ALL;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SecuritySubjectImpl.class, this);
        }
    }

    /** Represents {@link SecurityContext} implementation that ignores any security permission checks. */
    private static class SecurityContextImpl implements SecurityContext, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final SecuritySubject subj;

        /** */
        public SecurityContextImpl(UUID id, String login, SecuritySubjectType type, InetSocketAddress addr) {
            subj = new SecuritySubjectImpl(id, login, type, addr);
        }

        /** {@inheritDoc} */
        @Override public SecuritySubject subject() {
            return subj;
        }

        /** {@inheritDoc} */
        @Override public boolean taskOperationAllowed(String taskClsName, SecurityPermission perm) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean cacheOperationAllowed(String cacheName, SecurityPermission perm) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean serviceOperationAllowed(String srvcName, SecurityPermission perm) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean systemOperationAllowed(SecurityPermission perm) {
            return true;
        }
    }
}
