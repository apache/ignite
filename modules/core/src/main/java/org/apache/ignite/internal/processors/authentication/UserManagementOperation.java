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

package org.apache.ignite.internal.processors.authentication;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * The operation with users. Used to deliver the information about requested operation to all server nodes.
 */
public class UserManagementOperation implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** User. */
    private User usr;

    /** Operation type. */
    private OperationType type;

    /** Operation Id. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /**
     * Constructor.
     */
    public UserManagementOperation() {
        // No-op.
    }

    /**
     * @param usr User.
     * @param type Action type.
     */
    public UserManagementOperation(User usr, OperationType type) {
        this.usr = usr;
        this.type = type;
    }

    /**
     * @return User.
     */
    public User user() {
        return usr;
    }

    /**
     * @return Operation type.
     */
    public OperationType type() {
        return type;
    }

    /**
     * @return Operation ID.
     */
    public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserManagementOperation.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        UserManagementOperation op = (UserManagementOperation)o;

        return F.eq(id, op.id);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode();
    }

    /**
     * User action type.
     */
    public enum OperationType {ADD, UPDATE, REMOVE};
}
