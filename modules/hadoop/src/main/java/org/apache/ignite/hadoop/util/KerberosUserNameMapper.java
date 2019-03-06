/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
