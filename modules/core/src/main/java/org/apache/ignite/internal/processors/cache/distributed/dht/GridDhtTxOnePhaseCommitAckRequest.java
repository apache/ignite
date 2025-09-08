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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * One Phase Commit Near transaction ack request.
 */
public class GridDhtTxOnePhaseCommitAckRequest extends GridCacheMessage {
    /** Lock or transaction versions. */
    @GridToStringInclude
    @Order(value = 3, method = "versions")
    protected Collection<GridCacheVersion> vers;

    /**
     * Default constructor.
     */
    public GridDhtTxOnePhaseCommitAckRequest() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean cacheGroupMessage() {
        return false;
    }

    /**
     *
     * @param vers Near Tx xid Versions.
     */
    public GridDhtTxOnePhaseCommitAckRequest(Collection<GridCacheVersion> vers) {
        this.vers = vers;
    }

    /**
     * @return Lock or transaction versions.
     */
    public Collection<GridCacheVersion> versions() {
        return vers;
    }

    /**
     * @param vers Lock or transaction versions.
     */
    public void versions(Collection<GridCacheVersion> vers) {
        this.vers = vers;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxOnePhaseCommitAckRequest.class, this, super.toString());
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -27;
    }
}
