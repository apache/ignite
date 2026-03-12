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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot operation end request for {@link DistributedProcessType#END_SNAPSHOT} initiate message.
 */
public class SnapshotOperationEndRequest implements Message {
    /** Request ID. */
    @GridToStringInclude
    @Order(0)
    UUID reqId;

    /** Exception occurred during snapshot operation processing. */
    @Order(1)
    @Nullable ErrorMessage err;

    /**
     * Snapshot operation warnings. Warnings do not interrupt snapshot process but raise exception at the end to make
     * the operation status 'not OK' if no other error occurred.
     */
    @Order(2)
    @Nullable List<String> warnings;

    /** Default constructor for {@link MessageFactory}. */
    public SnapshotOperationEndRequest() {
        // No-op.
    }

    /**
     * @param id Request ID.
     * @param err Exception occurred during snapshot operation processing.
     * @param warnings Warnings of snapshot operation.
     */
    public SnapshotOperationEndRequest(UUID id, @Nullable Throwable err, @Nullable List<String> warnings) {
        reqId = id;
        this.err = new ErrorMessage(err);
        this.warnings = warnings;
    }

    /** @return Request ID. */
    public UUID requestId() {
        return reqId;
    }

    /** @return Exception occurred during snapshot operation processing. */
    @Nullable public Throwable error() {
        return ErrorMessage.error(err);
    }

    /** @return Warnings of snapshot operation. */
    @Nullable public List<String> warnings() {
        return warnings;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 36;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotOperationEndRequest.class, this, super.toString());
    }
}
