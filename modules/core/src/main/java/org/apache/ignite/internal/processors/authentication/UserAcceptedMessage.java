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
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Is sent as an acknowledgement for end (with success or error) of user management operation on the cluster
 * (see {@link UserProposedMessage} and {@link UserManagementOperation}).
 */
public class UserAcceptedMessage implements DiscoveryCustomMessage, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    private IgniteUuid id;

    /** Operation ID. */
    @Order(value = 1, method = "operationId")
    @GridToStringInclude
    private IgniteUuid opId;

    /** Error message. */
    @Order(value = 2, method = "errorMessage")
    private ErrorMessage errMsg;

    /** Constructor. */
    public UserAcceptedMessage() {
        // No-op.
    }

    /**
     * @param opId The ID of operation.
     * @param error Error.
     */
    UserAcceptedMessage(IgniteUuid opId, Throwable error) {
        assert opId != null || error != null;

        id = IgniteUuid.randomUuid();

        this.opId = opId;

        if (error != null)
            errMsg = new ErrorMessage(error);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /**
     * @param id Unique custom message ID.
     */
    public void id(IgniteUuid id) {
        this.id = id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer, DiscoCache discoCache) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return User operation ID.
     */
    public IgniteUuid operationId() {
        return opId;
    }

    /**
     * @param opId User operation ID.
     */
    public void operationId(IgniteUuid opId) {
        this.opId = opId;
    }

    /**
     * @return Error message.
     */
    public ErrorMessage errorMessage() {
        return errMsg;
    }

    /**
     * @param errMsg Error message.
     */
    public void errorMessage(ErrorMessage errMsg) {
        this.errMsg = errMsg;
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

    /** {@inheritDoc} */
    @Override public short directType() {
        return 500;
    }
}
