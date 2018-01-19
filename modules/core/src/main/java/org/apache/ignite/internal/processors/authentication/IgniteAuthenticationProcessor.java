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
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;

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
    }

    /**
     * Adds new user.
     *
     * @param login User's login.
     * @param passwd Plain text password.
     * @return User object.
     * @throws IgniteAuthenticationException On error.
     */
    public User addUser(String login, String passwd) throws IgniteAuthenticationException {
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
    public void removeUser(String login) throws IgniteAuthenticationException {
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
    public void changePassword(String login, String passwd) throws IgniteAuthenticationException {
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

        if (usr != null && usr.authorize(passwd))
            return usr;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        // TODO: init local storage
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        this.metastorage = metastorage;
    }
}
