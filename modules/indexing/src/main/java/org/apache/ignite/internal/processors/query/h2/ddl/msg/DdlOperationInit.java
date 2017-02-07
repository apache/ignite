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

package org.apache.ignite.internal.processors.query.h2.ddl.msg;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.query.h2.ddl.DdlOperationArguments;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * {@code INIT} part of a distributed DDL operation.
 */
public class DdlOperationInit implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Arguments. */
    private DdlOperationArguments args;

    /**
     * Map {@code node id} -> {@code init exception, if any}.
     * If this field is null, then this message is being processed by coordinator.
     */
    private Map<UUID, IgniteCheckedException> nodesState;

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        assert nodesState != null;

        Map<UUID, IgniteCheckedException> errors = new HashMap<>();

        for (Map.Entry<UUID, IgniteCheckedException> e : nodesState.entrySet())
            if (e.getValue() != null)
                errors.put(e.getKey(), e.getValue());

        // TODO: Can we send IO message to initiator asynchronously?
        if (!errors.isEmpty()) {
            DdlOperationInitError err = new DdlOperationInitError();

            err.setOperationId(args.opId);
            err.setErrors(errors);

            return err;
        }
        else {
            DdlOperationAck ackMsg = new DdlOperationAck();

            ackMsg.setOperationId(args.opId);

            return ackMsg;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /**
     * @return Operation arguments.
     */
    public DdlOperationArguments getArguments() {
        return args;
    }

    /**
     * @param args Operation arguments.
     */
    public void setArguments(DdlOperationArguments args) {
        this.args = args;
    }

    /**
     * @return Nodes state.
     */
    public Map<UUID, IgniteCheckedException> getNodesState() {
        return nodesState;
    }

    /**
     * @param nodesState Nodes state.
     */
    public void setNodesState(Map<UUID, IgniteCheckedException> nodesState) {
        this.nodesState = nodesState;
    }
}
