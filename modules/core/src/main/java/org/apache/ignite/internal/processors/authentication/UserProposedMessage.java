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

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryServerOnlyCustomMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * A node sends this message when it wants to propose user operation (add / update / remove).
 * <p>
 * After sending this message to the cluster sending node gets blocked until operation acknowledgement is received.
 * <p>
 * {@link UserAcceptedMessage} is sent as an acknowledgement that operation is finished on the all nodes of the cluster.
 */
public class UserProposedMessage extends DiscoveryServerOnlyCustomMessage {
    /** */
    @Order(0)
    @GridToStringInclude
    UserManagementOperation op;

    /** Constructor. */
    public UserProposedMessage() {
        // No-op.
    }

    /**
     * @param op User action.
     */
    UserProposedMessage(UserManagementOperation op) {
        super(IgniteUuid.randomUuid());

        assert op != null;

        this.op = op;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /**
     * @return User operation.
     */
    UserManagementOperation operation() {
        return op;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserProposedMessage.class, this);
    }
}
