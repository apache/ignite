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

package org.apache.ignite.internal.processors.query.h2.ddl;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class DdlOperationInitError implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** This message id. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** ID of whole DDL operation task at coordinator/initiator. */
    private IgniteUuid opId;

    /** Map from node IDs to their errors. */
    private Map<UUID, IgniteCheckedException> errors;

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }
    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /**
     * @return ID of whole DDL operation task at coordinator/initiator.
     */
    public IgniteUuid getOperationId() {
        return opId;
    }

    /**
     * @param opId ID of whole DDL operation task at coordinator/initiator.
     */
    public void setOperationId(IgniteUuid opId) {
        this.opId = opId;
    }

    /**
     * @return Map from node IDs to their errors.
     */
    public Map<UUID, IgniteCheckedException> getErrors() {
        return errors;
    }

    /**
     * @param errors Map from node IDs to their errors.
     */
    public void setErrors(Map<UUID, IgniteCheckedException> errors) {
        this.errors = errors;
    }
}
