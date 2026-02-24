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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Transaction finish response.
 */
public class GridDistributedTxFinishResponse extends GridCacheMessage {
    /** */
    @Order(3)
    public GridCacheVersion txId;

    /** Future ID. */
    @Order(4)
    public IgniteUuid futId;

    /** Partition ID this message is targeted to. */
    @Order(5)
    public int part;

    /**
     * Empty constructor required by {@link GridIoMessageFactory}.
     */
    public GridDistributedTxFinishResponse() {
        /* No-op. */
    }

    /**
     * @param part Partition.
     * @param txId Transaction id.
     * @param futId Future ID.
     */
    public GridDistributedTxFinishResponse(int part, GridCacheVersion txId, IgniteUuid futId) {
        assert txId != null;
        assert futId != null;

        this.part = part;
        this.txId = txId;
        this.futId = futId;
    }

    /** Partition ID this message is targeted to. */
    public void partition(int part) {
        this.part = part;
    }

    /** {@inheritDoc} */
    @Override public final int partition() {
        return part;
    }

    /**
     *
     * @return Transaction id.
     */
    public GridCacheVersion xid() {
        return txId;
    }

    /** */
    public void xid(GridCacheVersion txId) {
        this.txId = txId;
    }

    /** */
    public void futureId(IgniteUuid futId) {
        this.futId = futId;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext<?, ?> ctx) {
        return ctx.txFinishMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 24;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxFinishResponse.class, this);
    }
}
