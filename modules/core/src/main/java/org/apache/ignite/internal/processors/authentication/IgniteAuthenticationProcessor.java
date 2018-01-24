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
import org.apache.ignite.internal.processors.marshaller.MappingExchangeResult;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 */
public class IgniteAuthenticationProcessor extends GridProcessorAdapter implements MetastorageLifecycleListener {
    /** Default user. */
    private static final User DFLT_USER = User.create("ignite", "ignite");

    /** Default user. */
    private static final String STORE_USER_PREFIX = "user.";
    /** User operation history. */
    private final List<Long> history = new ArrayList<>();
    /** Map monitor. */
    private final Object mux = new Object();
    /** User exchange map. */
    private final ConcurrentMap<IgniteUuid, UserOperationFinishFuture> userOpFinishMap
        = new ConcurrentHashMap<>();
    /** User prepared map. */
    private final ConcurrentMap<String, UserPreparedFuture> usersPreparedMap = new ConcurrentHashMap<>();
    /** Futures prepared user map. */
    private final ConcurrentMap<IgniteUuid, UserPreparedFuture> futPreparedMap = new ConcurrentHashMap<>();
    /** User map. */
    private Map<String, User> users;
    /** Meta storage. */
    private ReadWriteMetastorage metastorage;

    /** Executor. */
    private IgniteThreadPoolExecutor exec;

    /** Coordinator node. */
    private ClusterNode crdNode;

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
     */
    private void cancelFutures() {
        for (UserOperationFinishFuture fut : userOpFinishMap.values())
            fut.onDone(UserOperationResult.createExchangeDisabledResult());

        for (UserPreparedFuture fut : usersPreparedMap.values())
            fut.onDone(null, new IgniteCheckedException("Node stopped."));
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
                fut.onSuccess(nodeId);
            else
                fut.onFail(nodeId, msg.errorMessage());
        }

    }

    /**
     * Adds new user locally.
     *
     * @param op User operation.
     * @throws IgniteAuthenticationException On error.
     */
    private void addUserLocal(final UserManagementOperation op) throws IgniteAuthenticationException {
        String userName = op.user().name();

        boolean futAlreadyCreate = false;

        synchronized (mux) {
            if (users.containsKey(userName)) {
                UserPreparedFuture prevFut = usersPreparedMap.get(userName);

                futAlreadyCreate = F.eq(prevFut.op, op);

                if (prevFut == null || prevFut.op.type() != UserManagementOperation.OperationType.REMOVE || !futAlreadyCreate)
                    throw new IgniteAuthenticationException("User already exists. [login=" + op.user().name() + ']');
            }

            if (!futAlreadyCreate) {
                UserPreparedFuture newFut = new UserPreparedFuture(op);

                futPreparedMap.put(op.id(), newFut);

                usersPreparedMap.put(op.user().name(), newFut);
            }

            exec.execute(new Runnable() {
                @Override public void run() {
                    storeUserLocal(op);
                }
            });
        }
    }

    /**
     * Store user to MetaStorage.
     *
     * @param op Operation.
     */
    private void storeUserLocal(UserManagementOperation op) {
        synchronized (mux) {
            try {
                User usr = op.user();

                metastorage.write(STORE_USER_PREFIX + usr.name(), usr);

                sendFinish(coordinator(), op.id(), null);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to store user. [op=" + op + ']', e);

                sendFinish(coordinator(), op.id(), e.toString());
            }
        }
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
     * Remove user from MetaStorage.
     *
     * @param op Operation.
     */
    private void removeUserLocal(UserManagementOperation op) {
        synchronized (mux) {
            try {
                User usr = op.user();

                metastorage.remove(STORE_USER_PREFIX + usr.name());

                sendFinish(coordinator(), op.id(), null);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to store user. [op=" + op + ']', e);

                sendFinish(coordinator(), op.id(), e.toString());
            }
        }
    }

    /**
     * Removes user.
     *
     * @param usr User to remove.
     * @throws IgniteAuthenticationException On error.
     */
    private void removeUserLocal(User usr) throws IgniteAuthenticationException {
        synchronized (mux) {
            if (!users.containsKey(usr.name()))
                throw new IgniteAuthenticationException("User not exists. [login=" + usr.name() + ']');

            users.remove(usr.name());
        }
    }

    /**
     * Change user password.
     *
     * @param usr User to update.
     * @throws IgniteAuthenticationException On error.
     */
    private void updateUserLocal(User usr) throws IgniteAuthenticationException {
        synchronized (mux) {
            if (!users.containsKey(usr.name()))
                throw new IgniteAuthenticationException("User not exists. [login=" + usr.name() + ']');

            users.put(usr.name(), usr);
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
    public User authenticate(String login, String passwd) throws IgniteCheckedException {
        User usr = null;

        UserPreparedFuture fut = usersPreparedMap.get(login);

        if (fut != null)
            usr = fut.get();

        if (usr == null) {
            synchronized (mux) {
                usr = users.get(login);
            }
        }

        if (usr == null)
            throw new IgniteAuthenticationException("The username or password is incorrect. [userName=" + login + ']');

        if (usr.authorize(passwd))
            return usr;
        else
            throw new IgniteAuthenticationException("The username or password is incorrect. [userName=" + login + ']');
    }

    /**
     * Adds new user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @return User object.
     * @throws IgniteAuthenticationException On error.
     */
    public User addUser(String login, String passwd) throws IgniteCheckedException {
        log.info("+++ addUser");

        UserManagementOperation op = new UserManagementOperation(User.create(login, passwd),
            UserManagementOperation.OperationType.ADD);

        UserPreparedFuture fut = new UserPreparedFuture(op);

        usersPreparedMap.putIfAbsent(login, fut);

        UserProposedMessage msg = new UserProposedMessage(
            new UserManagementOperation(User.create(login, passwd), UserManagementOperation.OperationType.ADD), ctx.localNodeId());

        ctx.discovery().sendCustomEvent(msg);

        return fut.get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        users = (Map<String, User>)metastorage.readForPredicate(new IgnitePredicate<String>() {
            @Override public boolean apply(String key) {
                return key != null && key.startsWith(STORE_USER_PREFIX);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        this.metastorage = metastorage;
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
    private void prepareOperationLocal(UserManagementOperation op) throws IgniteAuthenticationException {
        assert op != null && op.user() != null : "Invalid operation: " + op;

        switch (op.type()) {
            case ADD:
                addUserLocal(op);

                break;

            case REMOVE:
                removeUserLocal(op.user());

                break;

            case UPDATE:
                updateUserLocal(op.user());

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
     * @param result User operation result.
     */
    private void sendOperationAcknowledgement(UserOperationResult result) {
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
        synchronized void onSuccess(UUID nodeId) {
            receivedAcks.add(nodeId);

            checkOnDone();
        }

        /**
         * @param id Node ID.
         * @param errMsg Error message.
         */
        synchronized void onFail(UUID id, String errMsg) {
            receivedAcks.add(id);

            onDone(UserOperationResult.createFailureResult(
                new UserAuthenticationException("Operation failed. [nodeId=" + id + ", err=" + errMsg + ']', op)));
        }

        /**
         *
         */
        private void checkOnDone() {
            if (requiredAcks.equals(receivedAcks))
                onDone(UserOperationResult.createSuccessfulResult(op));
        }
    }

    /**
     * Future to wait for end of user operation.
     */
    private class UserPreparedFuture extends GridFutureAdapter<User> {
        /** User operation. */
        private UserManagementOperation op;

        /**
         * @param op User operation.
         */
        UserPreparedFuture(UserManagementOperation op) {
            this.op = op;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable User res, @Nullable Throwable err) {
            assert res != null;

            boolean done = super.onDone(res, err);

            if (done) {
                // Remove from user map if need
                usersPreparedMap.remove(op.user().name(), this);

                // Remove from all user futures map
                futPreparedMap.remove(op.id());
            }

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
            if (ctx.isStopping())
                return;

            log.info("+++ UserProposedMessage " + msg);

            synchronized (mux) {
                if (!msg.isError()) {
                    try {
                        if (isOnCoordinator()) {
                            UserOperationFinishFuture opFinishFut = new UserOperationFinishFuture(msg.operation());

                            userOpFinishMap.putIfAbsent(msg.operation().id(), opFinishFut);
                        }

                        prepareOperationLocal(msg.operation());
                    }
                    catch (IgniteCheckedException e) {
                        msg.error(e);
                    }
                }
                else {
                    UUID origNodeId = msg.origNodeId();

                    if (origNodeId.equals(ctx.localNodeId())) {
                        UserPreparedFuture fut = futPreparedMap.get(msg.operation().id());

                        assert fut != null : msg;

                        fut.onDone(null, msg.error());
                    }
                }
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

        }
    }
}
