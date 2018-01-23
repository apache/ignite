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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 */
public class IgniteAuthenticationProcessor extends GridProcessorAdapter implements MetastorageLifecycleListener {
    /** Default user. */
    private static final User DFLT_USER = User.create("ignite", "ignite");

    /** Default user. */
    private static final String STORE_USER_PREFIX = "user.";

    /** User map. */
    private final Map<String, User> users = new HashMap<>();

    /** User operation history. */
    private final List<Long> history = new ArrayList<>();

    /** Map monitor. */
    private final Object mux = new Object();

    /** User exchange map. */
    private final ConcurrentMap<UserManagementOperation, UserExchangeResultFuture> userExchMap
        = new ConcurrentHashMap<>();

    /** User prepared map. */
    private final ConcurrentMap<String, UserPreparedFuture> usersPreparedMap = new ConcurrentHashMap<>();

//    /** Futures prepared user map. */
//    private final ConcurrentMap<IgniteUuid, UserPreparedFuture> futPreparedMap = new ConcurrentHashMap<>();

    /** Meta storage. */
    private ReadWriteMetastorage metastorage;

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
    }

    /**
     * Adds new user locally.
     *
     * @param op User operation.
     * @throws IgniteAuthenticationException On error.
     */
    private void addUserLocal(UserManagementOperation op) throws IgniteAuthenticationException {
        synchronized (mux) {
            if (users.containsKey(op.user().name()))
                throw new IgniteAuthenticationException("User already exists. [login=" + op.user().name() + ']');

            if (usersPreparedMap.putIfAbsent(op.user().name(), new UserPreparedFuture(op)) != null)
                throw new IgniteAuthenticationException("Concurrent user modification. [login=" + op.user().name() + ']');
        }
    }

    /**
     * Store user to MetaStorage.
     *
     * @param op Operation.
     * @param node Cluster node to send acknowledgement.
     */
    private void storeUserLocal(UserManagementOperation op, ClusterNode node) {
        synchronized (mux) {
            try {
                User usr = op.user();

                metastorage.write(STORE_USER_PREFIX + usr.name(), usr);

                sendAck(node, op.id(), false, null);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to store user. [op=" + op + ']', e);

                sendAck(node, op.id(), false, e.toString());
            }
        }
    }

    /**
     * Remove user from MetaStorage.
     *
     * @param op Operation.
     * @param node Cluster node to send acknowledgement.
     */
    private void removeUserLocal(UserManagementOperation op, ClusterNode node) {
        synchronized (mux) {
            try {
                User usr = op.user();

                metastorage.remove(STORE_USER_PREFIX + usr.name());

                sendAck(node, op.id(), false, null);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to store user. [op=" + op + ']', e);

                sendAck(node, op.id(), false, e.toString());
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
        log.info("+++ addUser" );
        UserManagementOperation op = new UserManagementOperation(User.create(login, passwd),
            UserManagementOperation.OperationType.ADD);

        UserExchangeResultFuture fut = new UserExchangeResultFuture();

        userExchMap.putIfAbsent(op, fut);

        UserProposedMessage msg = new UserProposedMessage(
            new UserManagementOperation(User.create(login, passwd), UserManagementOperation.OperationType.ADD), ctx.localNodeId());

        ctx.discovery().sendCustomEvent(msg);

        return fut.get().operation().user();
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        // TODO: init local storage
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        this.metastorage = metastorage;
    }

    /**
     * Future to wait for mapping exchange result to arrive. Removes itself from map when completed.
     */
    private class UserExchangeResultFuture extends GridFutureAdapter<UserExchangeResult> {
        /** */
        private Collection<UUID> requiredAcks;

        /** */
        private Collection<UUID> receivedAcks;

        /**
         *
         */
        UserExchangeResultFuture() {
            requiredAcks = new HashSet<>();

            for (ClusterNode node : ctx.discovery().nodes(ctx.discovery().topologyVersionEx())) {
                if (!node.isClient() && !node.isDaemon())
                    requiredAcks.add(node.id());
            }

            receivedAcks = new HashSet<>();
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable UserExchangeResult res, @Nullable Throwable err) {
            assert res != null;

            boolean done = super.onDone(res, null);

            if (done)
                userExchMap.remove(res.operation(), this);

            return done;
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

            if (done)
                usersPreparedMap.remove(op.user().name(), this);

            return done;
        }
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
     * @param ack Ack flag.
     * @param err Error message.
     */
    private void sendAck(ClusterNode node, IgniteUuid opId, boolean ack, String err) {
        try {
            ctx.io().sendToGridTopic(node, GridTopic.TOPIC_USER,
                new UserManagementOperationFinishedMessage(opId, ack, err), GridIoPolicy.SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send UserManagementOperationFinishedMessage. [op=" + opId +
                ", node=" + node + ", ack=" + ack + ", err=" + err + ']', e);
        }
    }

    void q () {
//        ctx.pools().poolForPolicy(GridIoPolicy.SYSTEM_POOL).execute(new Runnable() {
//            @Override public void run() {
//                storeUserLocal(op, node);
//            }
//        });

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

            if (!msg.isError()) {
                try {
                    prepareOperationLocal(msg.operation());
                }
                catch (IgniteCheckedException e) {
                    msg.error(e);
                }
            }
            else {
                UUID origNodeId = msg.origNodeId();

                if (origNodeId.equals(ctx.localNodeId())) {
                    GridFutureAdapter<UserExchangeResult> fut = userExchMap.get(msg.operation());

                    assert fut != null: msg;

                    fut.onDone(UserExchangeResult.createFailureResult(msg.error()));
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
