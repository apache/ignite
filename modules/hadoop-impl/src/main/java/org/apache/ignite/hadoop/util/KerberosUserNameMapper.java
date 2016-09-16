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

package org.apache.ignite.hadoop.util;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

/**
 * Kerberos user name mapper. Use it when you need to map simple user name to Kerberos principal.
 * E.g. from {@code johndoe} to {@code johndoe@YOUR.REALM.COM} or {@code johndoe/admin@YOUR.REALM.COM}.
 */
public class KerberosUserNameMapper implements UserNameMapper, LifecycleAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Instance. */
    private String instance;

    /** Realm. */
    private String realm;

    /** State. */
    private volatile State state;

    /** {@inheritDoc} */
    @Nullable @Override public String map(String name) {
        assert state != null;

        name = IgfsUtils.fixUserName(name);

        switch (state) {
            case NAME:
                return name;

            case NAME_REALM:
                return name + '@' + realm;

            case NAME_INSTANCE:
                return name + '/' + instance;

            default:
                assert state == State.NAME_INSTANCE_REALM;

                return name + '/' + instance + '@' + realm;
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (!F.isEmpty(instance))
            state = F.isEmpty(realm) ? State.NAME_INSTANCE : State.NAME_INSTANCE_REALM;
        else
            state = F.isEmpty(realm) ? State.NAME : State.NAME_REALM;
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        // No-op.
    }

    /**
     * Get Kerberos instance (optional).
     *
     * @return Instance.
     */
    @Nullable public String getInstance() {
        return instance;
    }

    /**
     * Set Kerberos instance (optional).
     *
     * @param instance Kerberos instance.
     */
    public void setInstance(@Nullable String instance) {
        this.instance = instance;
    }

    /**
     * Get Kerberos realm (optional).
     *
     * @return Kerberos realm.
     */
    @Nullable public String getRealm() {
        return realm;
    }

    /**
     * Set Kerberos realm (optional).
     *
     * @param realm Kerberos realm.
     */
    public void setRealm(@Nullable String realm) {
        this.realm = realm;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(KerberosUserNameMapper.class, this);
    }

    /**
     * State enumeration.
     */
    private enum State {
        /** Name only. */
        NAME,

        /** Name and realm. */
        NAME_REALM,

        /** Name and host. */
        NAME_INSTANCE,

        /** Name, host and realm. */
        NAME_INSTANCE_REALM,
    }
}
