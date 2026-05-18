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
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Is sent as an acknowledgement for end (with success or error) of user management operation on the cluster
 * (see {@link UserProposedMessage} and {@link UserManagementOperation}).
 */
public class UserAcceptedMessage extends DiscoveryCustomMessage {
    /** Operation ID. */
    @Order(0)
    @GridToStringInclude
    IgniteUuid opId;

    /** Error message. */
    @Order(1)
    ErrorMessage errMsg;

    /** Constructor. */
    public UserAcceptedMessage() {
        // No-op.
    }

    /**
     * @param opId The ID of operation.
     * @param error Error.
     */
    UserAcceptedMessage(IgniteUuid opId, Throwable error) {
        super(IgniteUuid.randomUuid());

        assert opId != null || error != null;

        this.opId = opId;

        if (error != null)
            errMsg = new ErrorMessage(error);
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /**
     * @return User operation ID.
     */
    IgniteUuid operationId() {
        return opId;
    }

    /**
     * @return Error.
     */
    Throwable error() {
        return ErrorMessage.error(errMsg);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserAcceptedMessage.class, this);
    }
}
