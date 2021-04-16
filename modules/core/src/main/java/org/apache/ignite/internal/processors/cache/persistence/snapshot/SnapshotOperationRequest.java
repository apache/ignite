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

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot operation start request for {@link DistributedProcess} initiate message.
 */
public class SnapshotOperationRequest implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Request ID. */
    private final UUID reqId;

    /** Snapshot name. */
    private final String snpName;

    /** Baseline node IDs that must be alive to complete the operation. */
    @GridToStringInclude
    private final Set<UUID> nodes;

    /** List of cache group names. */
    @GridToStringInclude
    private final Collection<String> grps;

    /** Operational node ID. */
    private final UUID opNodeId;

    /** Exception occurred during snapshot operation processing. */
    private volatile Throwable err;

    /**
     * @param reqId Request ID.
     * @param opNodeId Operational node ID.
     * @param snpName Snapshot name.
     * @param grps List of cache group names.
     * @param nodes Baseline node IDs that must be alive to complete the operation.
     */
    public SnapshotOperationRequest(
        UUID reqId,
        UUID opNodeId,
        String snpName,
        @Nullable Collection<String> grps,
        Set<UUID> nodes
    ) {
        this.reqId = reqId;
        this.opNodeId = opNodeId;
        this.snpName = snpName;
        this.grps = grps;
        this.nodes = nodes;
    }

    /**
     * @return Request ID.
     */
    public UUID requestId() {
        return reqId;
    }

    /**
     * @return Snapshot name.
     */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @return List of cache group names.
     */
    public @Nullable Collection<String> groups() {
        return grps;
    }

    /**
     * @return Baseline node IDs that must be alive to complete the operation.
     */
    public Set<UUID> nodes() {
        return nodes;
    }

    /**
     * @return Operational node ID.
     */
    public UUID operationalNodeId() {
        return opNodeId;
    }

    /**
     * @return Exception occurred during snapshot operation processing.
     */
    public Throwable error() {
        return err;
    }

    /**
     * @param err Exception occurred during snapshot operation processing.
     */
    public void error(Throwable err) {
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotOperationRequest.class, this);
    }
}
