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
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxStateAware;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.jetbrains.annotations.Nullable;

/**
 * Response to prepare request.
 */
public class GridDistributedTxPrepareResponse extends GridDistributedBaseMessage implements IgniteTxStateAware {
    /** Error message. */
    @GridToStringExclude
    @Order(7)
    @Nullable public ErrorMessage errMsg;

    /** TX state. */
    private IgniteTxState txState;

    /** Partition ID this message is targeted to. */
    @Order(8)
    public int part;

    /**
     * Empty constructor.
     */
    public GridDistributedTxPrepareResponse() {
        /* No-op. */
    }

    /**
     * @param part Partition.
     * @param xid Lock or transaction ID.
     * @param addDepInfo Deployment info flag.
     */
    public GridDistributedTxPrepareResponse(int part, GridCacheVersion xid, boolean addDepInfo) {
        super(xid, 0, addDepInfo);

        this.part = part;
    }

    /**
     * @param part Partition.
     * @param xid Lock or transaction ID.
     * @param err Error.
     * @param addDepInfo Deployment info flag.
     */
    public GridDistributedTxPrepareResponse(int part, GridCacheVersion xid, @Nullable Throwable err, boolean addDepInfo) {
        super(xid, 0, addDepInfo);

        this.part = part;

        if (err != null)
            errMsg = new ErrorMessage(err);
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return part;
    }

    /**
     * @param part New Partition ID this message is targeted to.
     */
    public void partition(int part) {
        this.part = part;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Throwable error() {
        return ErrorMessage.error(errMsg);
    }

    /**
     * @param err Error to set.
     */
    public void error(@Nullable Throwable err) {
        if (err != null)
            errMsg = new ErrorMessage(err);
    }

    /** {@inheritDoc} */
    @Override public IgniteTxState txState() {
        return txState;
    }

    /** {@inheritDoc} */
    @Override public void txState(IgniteTxState txState) {
        this.txState = txState;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext<?, ?> ctx) {
        return ctx.txPrepareMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 26;
    }

    /**
     * @return Error message.
     */
    public @Nullable ErrorMessage errorMessage() {
        return errMsg;
    }

    /**
     * @param errMsg New error message.
     */
    public void errorMessage(@Nullable ErrorMessage errMsg) {
        this.errMsg = errMsg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxPrepareResponse.class, this, "err",
            error() == null ? "null" : error().toString(), "super", super.toString());
    }
}
