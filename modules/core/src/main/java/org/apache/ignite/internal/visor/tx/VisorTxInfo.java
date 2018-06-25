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

package org.apache.ignite.internal.visor.tx;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

/**
 */
public class VisorTxInfo extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /** */
    private IgniteUuid xid;

    /**
     * Transaction start time.
     */
    private long startTime;

    /** */
    private long duration;

    /** */
    private TransactionIsolation isolation;

    /** */
    private TransactionConcurrency concurrency;

    /** */
    private long timeout;

    /** */
    private String lb;

    /** */
    private Collection<UUID> primaryNodes;

    /** */
    private TransactionState state;

    /** */
    private int size;

    /** */
    private IgniteUuid nearXid;

    /** */
    private Collection<UUID> masterNodeIds;

    /**
     * Default constructor.
     */
    public VisorTxInfo() {
        // No-op.
    }

    /**
     * @param xid Xid.
     * @param startTime Start time of transaction.
     * @param duration Duration.
     * @param isolation Isolation.
     * @param concurrency Concurrency.
     * @param timeout Timeout.
     * @param lb Label.
     * @param primaryNodes Primary nodes.
     * @param state State.
     * @param size Size.
     */
    public VisorTxInfo(IgniteUuid xid, long startTime, long duration, TransactionIsolation isolation,
        TransactionConcurrency concurrency, long timeout, String lb, Collection<UUID> primaryNodes,
        TransactionState state, int size, IgniteUuid nearXid, Collection<UUID> masterNodeIds) {
        this.xid = xid;
        this.startTime = startTime;
        this.duration = duration;
        this.isolation = isolation;
        this.concurrency = concurrency;
        this.timeout = timeout;
        this.lb = lb;
        this.primaryNodes = primaryNodes;
        this.state = state;
        this.size = size;
        this.nearXid = nearXid;
        this.masterNodeIds = masterNodeIds;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** */
    public IgniteUuid getXid() {
        return xid;
    }

    /** */
    public long getStartTime() {
        return startTime;
    }

    /** */
    public String getFormattedStartTime() {
        return dateTimeFormatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(startTime), TimeZone.getDefault().toZoneId()));
    }

    /** */
    public long getDuration() {
        return duration;
    }

    /** */
    public TransactionIsolation getIsolation() {
        return isolation;
    }

    /** */
    public TransactionConcurrency getConcurrency() {
        return concurrency;
    }

    /** */
    public long getTimeout() {
        return timeout;
    }

    /** */
    public String getLabel() {
        return lb;
    }

    /** */
    public Collection<UUID> getPrimaryNodes() {
        return primaryNodes;
    }

    /** */
    public TransactionState getState() {
        return state;
    }

    /** */
    public int getSize() {
        return size;
    }

    /** */
    public @Nullable IgniteUuid getNearXid() {
        return nearXid;
    }

    /** */
    public @Nullable Collection<UUID> getMasterNodeIds() {
        return masterNodeIds;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, xid);
        out.writeLong(duration);
        U.writeEnum(out, isolation);
        U.writeEnum(out, concurrency);
        out.writeLong(timeout);
        U.writeString(out, lb);
        U.writeCollection(out, primaryNodes);
        U.writeEnum(out, state);
        out.writeInt(size);
        U.writeGridUuid(out, nearXid);
        U.writeCollection(out, masterNodeIds);
        out.writeLong(startTime);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        xid = U.readGridUuid(in);
        duration = in.readLong();
        isolation = TransactionIsolation.fromOrdinal(in.readByte());
        concurrency = TransactionConcurrency.fromOrdinal(in.readByte());
        timeout = in.readLong();
        lb = U.readString(in);
        primaryNodes = U.readCollection(in);
        state = TransactionState.fromOrdinal(in.readByte());
        size = in.readInt();
        if (protoVer >= V2) {
            nearXid = U.readGridUuid(in);

            masterNodeIds = U.readCollection(in);

            startTime = in.readLong();
        }

    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorTxInfo.class, this);
    }
}
