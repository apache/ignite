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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.AUTH_PROC;

/**
 */
public class IgniteAuthenticationProcessor extends GridProcessorAdapter implements MetastorageLifecycleListener {
    /** Store user prefix. */
    private static final String STORE_USER_PREFIX = "user.";

    /** Store users info version. */
    private static final String STORE_USERS_VERSION = "users-version";

    /** User operation history. */
    private final List<Long> history = new ArrayList<>();

    /** Map monitor. */
    private final Object mux = new Object();

    /** User exchange map. */
    private final ConcurrentMap<IgniteUuid, UserOperationFinishFuture> userOpFinishMap
        = new ConcurrentHashMap<>();

    /** Futures prepared user map. */
    private final ConcurrentMap<IgniteUuid, UserOperationFuture> opFuts = new ConcurrentHashMap<>();

    /** Futures prepared user map. */
    private final ConcurrentMap<IgniteUuid, GridFutureAdapter<User>> authFuts = new ConcurrentHashMap<>();

    /** Active operations. */
    private List<UserManagementOperation> activeOperations = new ArrayList<>();

    /** User map. */
    private Map<String, User> users;

    /** Meta storage. */
    private ReadWriteMetastorage metastorage;

    /** Executor. */
    private IgniteThreadPoolExecutor exec;

    /** Coordinator node. */
    private ClusterNode crdNode;

    /** Users info version. */
    private long usersInfoVersion = 0;

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

        synchronized (mux) {
            GridDiscoveryManager discoMgr = ctx.discovery();
            GridIoManager ioMgr = ctx.io();

            discoMgr.setCustomEventListener(UserProposedMessage.class, new UserProposedListener());

            discoMgr.setCustomEventListener(UserAcceptedMessage.class, new UserAcceptedListener());

            exec = new IgniteThreadPoolExecutor(
                "auth",
                ctx.config().getIgniteInstanceName(),
                1,
                1,
                0,
                new LinkedBlockingQueue<>());

            ctx.event().addLocalEventListener(new GridLocalEventListener() {
                @Override public void onEvent(Event evt) {
                    DiscoveryEvent evt0 = (DiscoveryEvent)evt;

                    if (!ctx.isStopping()) {
                        synchronized (mux) {
                            crdNode = null;

                            if (isOnCoordinator()) {
                                for (UserOperationFinishFuture f : userOpFinishMap.values())
                                    f.onNodeLeft(evt0.eventNode().id());
                            }
                        }
                    }
                }
            }, EVT_NODE_LEFT, EVT_NODE_FAILED);

            ioMgr.addMessageListener(GridTopic.TOPIC_AUTH, new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                    if (msg instanceof UserManagementOperationFinishedMessage)
                        onFinishMessage(nodeId, (UserManagementOperationFinishedMessage)msg);
                    else if (msg instanceof UserAuthenticateRequestMessage)
                        onAuthenticateRequestMessage(nodeId, (UserAuthenticateRequestMessage)msg);
                    else if (msg instanceof UserAuthenticateResponseMessage)
                        onAuthenticateResponseMessage((UserAuthenticateResponseMessage)msg);
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (!cancel)
            exec.shutdown();
        else
            exec.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        cancelFutures();
    }

    /**
     * Get current coordinator node.
     *
     * @return Coordinator node.
     */
    private ClusterNode coordinator() {
        assert Thread.holdsLock(mux);

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

    /**
     */
    private void cancelFutures() {
        for (UserOperationFinishFuture fut : userOpFinishMap.values())
            fut.onDone(UserOperationResult.createExchangeDisabledResult());
    }

    /**
     * Adds new user locally.
     *
     * @param op User operation.
     * @throws IgniteAuthenticationException On error.
     */
    private void addUserLocal(final UserManagementOperation op) throws IgniteCheckedException {
        assert Thread.holdsLock(mux);

        User usr = op.user();

        String userName = usr.name();

        if (users.containsKey(userName))
            throw new IgniteAuthenticationException("User already exists. [login=" + userName + ']');

        metastorage.write(STORE_USER_PREFIX + userName, usr);

        updateUsersVersion();

        users.put(userName, usr);
    }

    /**
     * @throws IgniteCheckedException On error.
     */
    private void updateUsersVersion() throws IgniteCheckedException {
        metastorage.write(STORE_USERS_VERSION, usersInfoVersion + 1);

        usersInfoVersion++;
    }

    /**
     * Remove user from MetaStorage.
     *
     * @param op Operation.
     * @throws IgniteCheckedException On error.
     */
    private void removeUserLocal(UserManagementOperation op) throws IgniteCheckedException {
        assert Thread.holdsLock(mux);

        User usr = op.user();

        if (!users.containsKey(usr.name()))
            throw new IgniteAuthenticationException("User not exists. [login=" + usr.name() + ']');

        metastorage.remove(STORE_USER_PREFIX + usr.name());

        updateUsersVersion();

        users.remove(usr.name());
    }

    /**
     * Remove user from MetaStorage.
     *
     * @param op Operation.
     * @throws IgniteCheckedException On error.
     */
    private void updateUserLocal(UserManagementOperation op) throws IgniteCheckedException {
        assert Thread.holdsLock(mux);

        User usr = op.user();

        if (!users.containsKey(usr.name()))
            throw new IgniteAuthenticationException("User not exists. [login=" + usr.name() + ']');

        metastorage.write(STORE_USER_PREFIX + usr.name(), usr);

        updateUsersVersion();

        users.put(usr.name(), usr);

        sendFinish(coordinator(), op.id(), null);
    }

    /**
     * Authenticate user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @return User object on successful authenticate. Otherwise returns {@code null}.
     * @throws IgniteCheckedException On authentication error.
     */
    public User authenticate(String login, String passwd) throws IgniteCheckedException {
        if (ctx.clientNode()) {
            GridFutureAdapter<User> fut = new GridFutureAdapter<>();

            UserAuthenticateRequestMessage msg = new UserAuthenticateRequestMessage(login, passwd);

            authFuts.put(msg.id(), fut);

            synchronized (mux) {
                ctx.io().sendToGridTopic(coordinator(), GridTopic.TOPIC_AUTH, msg, GridIoPolicy.SYSTEM_POOL);
            }

            return fut.get();
        }
        else {
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
    }

    /**
     * Adds new user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @throws IgniteAuthenticationException On error.
     */
    public void addUser(String login, String passwd) throws IgniteCheckedException {
        UserManagementOperation op = new UserManagementOperation(User.create(login, passwd),
            UserManagementOperation.OperationType.ADD);

        execUserOperation(op);
    }

    /**
     * @param login User name.
     * @throws IgniteCheckedException On error.
     */
    public void removeUser(String login) throws IgniteCheckedException {
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
        UserManagementOperation op = new UserManagementOperation(User.create(login, passwd),
            UserManagementOperation.OperationType.UPDATE);

        execUserOperation(op);
    }

    /**
     * @param op User operation.
     * @throws IgniteCheckedException On error.
     */
    private void execUserOperation(UserManagementOperation op) throws IgniteCheckedException {
        UserOperationFuture fut = new UserOperationFuture(op);

        opFuts.putIfAbsent(op.id(), fut);

        UserProposedMessage msg = new UserProposedMessage(op, ctx.localNodeId());

        ctx.discovery().sendCustomEvent(msg);

        log.info("+++ WAIT " + op);

        fut.get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        synchronized (mux) {
            if (!ctx.clientNode()) {
                Long ver = (Long)metastorage.read(STORE_USERS_VERSION);
                usersInfoVersion = ver == null ? 0 : ver;

                users = (Map<String, User>)metastorage.readForPredicate(new IgnitePredicate<String>() {
                    @Override public boolean apply(String key) {
                        return key != null && key.startsWith(STORE_USER_PREFIX);
                    }
                });
            }
            else
                users = null;
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
                dataBag.addGridCommonData(AUTH_PROC.ordinal(), new UsersData(users.values(), activeOperations, ver));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        UsersData usersData = (UsersData)data.commonData();

        synchronized (mux) {
            if (usersData != null) {
                exec.execute(new RefreshUsersStorageWorker(usersData.usrs));

                for (UserManagementOperation op : usersData.activeOps)
                    exec.execute(new UserOperationWorker(op));
            }
        }
    }

    /**
     *
     */
    private void addDefaultUser() {
        synchronized (mux) {
            User u = User.defaultUser();

        }
    }

    /**
     * @return Runs on coordinator flag.
     */
    private boolean isOnCoordinator() {
        return F.eq(ctx.localNodeId(), coordinator().id());
    }

    /**
     * @param op The operation with users.
     * @throws IgniteAuthenticationException On authentication error.
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
     * @param node Node to sent acknowledgement.
     * @param opId Operation ID.
     * @param err Error message.
     */
    private void sendFinish(ClusterNode node, IgniteUuid opId, String err) {
        try {
            ctx.io().sendToGridTopic(node, GridTopic.TOPIC_AUTH,
                new UserManagementOperationFinishedMessage(opId, err), GridIoPolicy.SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send UserManagementOperationFinishedMessage. [op=" + opId +
                ", node=" + node + ", err=" + err + ']', e);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void onFinishMessage(UUID nodeId, UserManagementOperationFinishedMessage msg) {
        UserOperationFinishFuture fut = userOpFinishMap.get(msg.operationId());

        if (fut == null)
            log.warning("Not found appropriate user operation. [msg=" + msg + ']');
        else {
            if (msg.success())
                fut.onSuccessOnNode(nodeId);
            else
                fut.onOperationFailOnNode(nodeId, msg.errorMessage());
        }
    }

    /**
     * @param res Finish operation result.
     */
    private void onFinishOperation(UserOperationResult res) {
        assert res != null;

        try {
            UserAcceptedMessage msg = new UserAcceptedMessage(res.operationId(), res.error());

            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Unexpected exception.", e);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void onAuthenticateRequestMessage(UUID nodeId, UserAuthenticateRequestMessage msg) {
        UserAuthenticateResponseMessage respMsg;
        try {
            User u = authenticate(msg.name(), msg.password());

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
            U.error(log, "Unexpected exception.", e);
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
     * Future to wait for mapping exchange result to arrive. Removes itself from map when completed.
     */
    private class UserOperationFinishFuture extends GridFutureAdapter<UserOperationResult> {
        /** */
        private final Collection<UUID> requiredAcks;

        /** */
        private final Collection<UUID> receivedAcks;

        /** User management operation. */
        private final UserManagementOperation op;

        /**
         * @param op User management operation.
         */
        UserOperationFinishFuture(UserManagementOperation op) {
            this.op = op;

            requiredAcks = new HashSet<>();

            for (ClusterNode node : ctx.discovery().nodes(ctx.discovery().topologyVersionEx())) {
                if (!node.isClient() && !node.isDaemon())
                    requiredAcks.add(node.id());
            }

            receivedAcks = new HashSet<>();
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable UserOperationResult res, @Nullable Throwable err) {
            assert res != null;

            boolean done = super.onDone(res, err);

            if (done)
                userOpFinishMap.remove(op.id(), this);

            return done;
        }

        /**
         * @param nodeId ID of left or failed node.
         */
        synchronized void onNodeLeft(UUID nodeId) {
            requiredAcks.remove(nodeId);

            checkOnDone();
        }

        /**
         * @param nodeId Node ID.
         */
        synchronized void onSuccessOnNode(UUID nodeId) {
            receivedAcks.add(nodeId);

            checkOnDone();
        }

        /**
         * @param id Node ID.
         * @param errMsg Error message.
         */
        synchronized void onOperationFailOnNode(UUID id, String errMsg) {
            receivedAcks.add(id);

            onDone(UserOperationResult.createFailureResult(
                new UserAuthenticationException("Operation failed. [nodeId=" + id + ", err=" + errMsg + ']', op)));
        }

        /**
         *
         */
        private void checkOnDone() {
            if (requiredAcks.equals(receivedAcks))
                onDone(UserOperationResult.createSuccessfulResult(op.id()));
        }
    }

    /**
     *
     */
    private class RefreshUsersStorageWorker extends GridWorker {
        private final Collection<User> newUsrs;

        /**
         * @param usrs New users to store.
         */
        private RefreshUsersStorageWorker(Collection<User> usrs) {
            super(ctx.igniteInstanceName(), "refresh-store", IgniteAuthenticationProcessor.this.log);

            newUsrs = usrs;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            synchronized (mux) {
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

                        users.put(u.name(), u);
                    }
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Cannot cleanup old users information at metastorage", e);
                }
            }
        }
    }

    /**
     * WAL state change worker.
     */
    private class UserOperationWorker extends GridWorker {
        /** Message. */
        private final UserManagementOperation op;

        /**
         * Constructor.
         *
         * @param op Operation.
         */
        private UserOperationWorker(UserManagementOperation op) {
            super(ctx.igniteInstanceName(), "auth-op-" + op.type(), IgniteAuthenticationProcessor.this.log);

            this.op = op;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            UserOperationFuture fut = new UserOperationFuture(op);

            synchronized (mux) {
                UserOperationFuture futPrev = opFuts.putIfAbsent(op.id(), fut);

                if (futPrev != null)
                    fut = futPrev;

                try {
                    processOperationLocal(op);

                    log.info("+++ Finish " + op);

                    sendFinish(coordinator(), op.id(), null);
                }
                catch (IgniteCheckedException e) {
                    log.info("+++ Finish " + op);

                    sendFinish(coordinator(), op.id(), e.toString());
                }
            }

            try {
                log.info("+++ Worker wait " + op.id());
                fut.get();
                log.info("+++ Worker end " + op.id());
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Unexpected exception.", e);
            }
        }
    }

    /**
     * Future to wait for end of user operation.
     */
    private class UserOperationFuture extends GridFutureAdapter<Void> {
        /** User operation. */
        private UserManagementOperation op;

        /**
         * @param op User operation.
         */
        UserOperationFuture(UserManagementOperation op) {
            this.op = op;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
            boolean done = super.onDone(res, err);

            if (done)
                opFuts.remove(op.id());

            return done;
        }
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

            log.info("+++ UserProposedMessage " + msg);

            synchronized (mux) {
                if (isOnCoordinator()) {
                    UserOperationFinishFuture opFinishFut = new UserOperationFinishFuture(msg.operation());

                    opFinishFut.listen(new IgniteInClosure<IgniteInternalFuture<UserOperationResult>>() {
                        @Override public void apply(IgniteInternalFuture<UserOperationResult> f) {
                            onFinishOperation(f.result());
                        }
                    });

                    userOpFinishMap.putIfAbsent(msg.operation().id(), opFinishFut);
                }

                exec.execute(new UserOperationWorker(msg.operation()));
            }
        }
    }

    /**
     *
     */
    private final class UserAcceptedListener implements CustomEventListener<UserAcceptedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, UserAcceptedMessage msg) {
            log.info("+++ UserAcceptedMessage " + msg);

            UserOperationFuture f = opFuts.get(msg.operationId());

            if (f != null) {
                log.info("+++ DONE " + f.op.id());

                f.onDone(null, msg.error());
            }
            else {
                log.info("+++ Not found operation: " + msg.operationId());

            }
        }
    }

    /**
     *
     */
    private final class UsersData implements Serializable {
        /** Users. */
        private final List<User> usrs;

        /** Active user operations. */
        private final List<UserManagementOperation> activeOps;

        /** Users information version. */
        private final long usrVer;

        /**
         * @param usrs Users.
         * @param ops Active operations on cluster.
         * @param ver
         */
        UsersData(Collection<User> usrs, List<UserManagementOperation> ops, long ver) {
            this.usrs = new ArrayList(usrs);
            activeOps = ops;
            usrVer = ver;
        }
    }
}
