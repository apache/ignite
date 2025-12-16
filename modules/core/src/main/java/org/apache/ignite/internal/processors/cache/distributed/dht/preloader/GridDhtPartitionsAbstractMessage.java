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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Request for single partition info.
 */
public abstract class GridDhtPartitionsAbstractMessage extends GridCacheMessage {
    /** */
    private static final byte RESTORE_STATE_FLAG_MASK = 0x02;

    /** Exchange ID. */
    @Order(value = 3, method = "exchangeId")
    private GridDhtPartitionExchangeId exchId;

    /** Last used cache version. */
    @Order(value = 4, method = "lastVersion")
    private GridCacheVersion lastVer;

    /** */
    @Order(5)
    protected byte flags;

    /**
     * Empty constructor.
     */
    protected GridDhtPartitionsAbstractMessage() {
        // No-op.
    }

    /**
     * @param exchId Exchange ID.
     * @param lastVer Last version.
     */
    GridDhtPartitionsAbstractMessage(GridDhtPartitionExchangeId exchId, @Nullable GridCacheVersion lastVer) {
        this.exchId = exchId;
        this.lastVer = lastVer;
    }

    /**
     * @param msg Message.
     */
    void copyStateTo(GridDhtPartitionsAbstractMessage msg) {
        msg.exchId = exchId;
        msg.lastVer = lastVer;
        msg.flags = flags;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return GridIoMessage.STRIPE_DISABLED_PART;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean partitionExchangeMessage() {
        return true;
    }

    /**
     * @return Exchange ID. {@code Null} if message doesn't belong to exchange process.
     */
    @Nullable public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @param exchId Exchange ID.
     */
    public void exchangeId(GridDhtPartitionExchangeId exchId) {
        this.exchId = exchId;
    }

    /**
     * @return Last used version among all nodes.
     */
    @Nullable public GridCacheVersion lastVersion() {
        return lastVer;
    }

    /**
     * @param lastVer Last used version among all nodes.
     */
    public void lastVersion(GridCacheVersion lastVer) {
        this.lastVer = lastVer;
    }

    /**
     * @return Flags.
     */
    public byte flags() {
        return flags;
    }

    /**
     * @param flags Flags.
     */
    public void flags(byte flags) {
        this.flags = flags;
    }

    /**
     * @param restoreState Restore exchange state flag.
     */
    void restoreState(boolean restoreState) {
        flags = restoreState ? (byte)(flags | RESTORE_STATE_FLAG_MASK) : (byte)(flags & ~RESTORE_STATE_FLAG_MASK);
    }

    /**
     * @return Restore exchange state flag.
     */
    public boolean restoreState() {
        return (flags & RESTORE_STATE_FLAG_MASK) != 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsAbstractMessage.class, this, super.toString());
    }
}
