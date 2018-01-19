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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

/**
 */
public class IgniteAuthenticationProcessor extends GridProcessorAdapter implements MetastorageLifecycleListener {
    /** Default user. */
    private static final User DFLT_USER = User.create("ignite", "ignite");

    /** User map. */
    private final Map<String, User> users = new HashMap<>();

    /** User map. */
    private final List<Long> history = new ArrayList<>();

    /** Map monitor. */
    private final Object mux = new Object();

    /** User exchange map. */
    private final ConcurrentMap<UserManagementOperation, GridFutureAdapter<UserExchangeResult>> userExchMap
        = new ConcurrentHashMap<>();

    /** Metastorage. */
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
     * @param login User's login.
     * @param passwd Plain text password.
     * @return User object.
     * @throws IgniteAuthenticationException On error.
     */
    private User addUserLocal(String login, String passwd) throws IgniteAuthenticationException {
        synchronized (mux) {
            if (users.containsKey(login))
                throw new IgniteAuthenticationException("User already exists. [login=" + login + ']');

            User usr = User.create(login, passwd);

            users.put(login, usr);

            return usr;
        }
    }

    /**
     * Removes user.
     *
     * @param login User's login.
     * @throws IgniteAuthenticationException On error.
     */
    private void removeUserLocal(String login) throws IgniteAuthenticationException {
        synchronized (mux) {
            if (!users.containsKey(login))
                throw new IgniteAuthenticationException("User not exists. [login=" + login + ']');

            users.remove(login);
        }
    }

    /**
     * Change user password.
     *
     * @param login User's login.
     * @param passwd New password.
     * @throws IgniteAuthenticationException On error.
     */
    private void changePasswordLocal(String login, String passwd) throws IgniteAuthenticationException {
        synchronized (mux) {
            if (!users.containsKey(login))
                throw new IgniteAuthenticationException("User not exists. [login=" + login + ']');

            User usr = User.create(login, passwd);

            users.put(login, usr);
        }
    }

    /**
     * Authenticate user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @return User object on successful authenticate. Otherwise returns {@code null}.
     */
    public User authenticate(String login, String passwd) {
        User usr = null;

        synchronized (mux) {
            usr = users.get(login);
        }

        if (usr != null) {
            if (!usr.accepted()) {
                // TODO: wait cluster accept
            }

            if (usr.authorize(passwd))
                return usr;
        }

        return null;
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

        GridFutureAdapter<UserExchangeResult> fut = new UserExchangeResultFuture();

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
                    ctx.pools().poolForPolicy(GridIoPolicy.SYSTEM_POOL).execute(new Runnable() {
                        @Override public void run() {
                            try {

                            } catch (Throwable t) {

                            }
                        }
                    });
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
