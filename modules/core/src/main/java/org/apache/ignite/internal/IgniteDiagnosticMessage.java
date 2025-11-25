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

package org.apache.ignite.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteDiagnosticMessage implements Message {
    /** */
    @Order(value = 0, method = "infoResponse")
    private @Nullable String infoResp;

    /** */
    @Order(value = 1, method = "futureId")
    private long futId;

    /** */
    @Order(2)
    private IgniteCompoundDiagnosicInfo compoundInfo;

    /**
     * Required by {@link GridIoMessageFactory}.
     */
    public IgniteDiagnosticMessage() {
        // No-op.
    }

    /**
     * @param info Compound info.
     * @param futId Future ID.
     * @return Request message.
     * @throws IgniteCheckedException If failed.
     */
    public static IgniteDiagnosticMessage createRequest(IgniteCompoundDiagnosicInfo info, long futId) throws IgniteCheckedException {
        IgniteDiagnosticMessage msg = new IgniteDiagnosticMessage();

        msg.futId = futId;
        msg.compoundInfo = info;

        return msg;
    }

    /**
     * @param diagnosticInfo Diagnostic info result.
     * @param futId Future ID.
     * @return Response message.
     */
    public static IgniteDiagnosticMessage createResponse(String diagnosticInfo, long futId) {
        IgniteDiagnosticMessage msg = new IgniteDiagnosticMessage();

        msg.futId = futId;
        msg.infoResp = diagnosticInfo;

        return msg;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /** */
    public void futureId(long futId) {
        this.futId = futId;
    }

    /**
     * @return {@code True} if this is request message.
     */
    public boolean request() {
        return infoResp == null;
    }

    /** */
    public @Nullable String infoResponse() {
        return infoResp;
    }

    /** */
    public void infoResponse(@Nullable String infoResp) {
        this.infoResp = infoResp;
    }

    /** */
    public IgniteCompoundDiagnosicInfo compoundInfo() {
        return compoundInfo;
    }

    /** */
    public void compoundInfo(IgniteCompoundDiagnosicInfo compoundInfo) {
        this.compoundInfo = compoundInfo;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -61;
    }

    /**
     *
     */
    public abstract static class DiagnosticBaseInfo implements Externalizable, Message {
        /**
         * @param other Another info of the same type.
         */
        public void merge(DiagnosticBaseInfo other) {
            // No-op.
        }

        /**
         * @param sb String builder.
         * @param ctx Grid context.
         */
        public abstract void appendInfo(StringBuilder sb, GridKernalContext ctx);
    }

    /**
     *
     */
    public static final class TxInfo extends DiagnosticBaseInfo {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private GridCacheVersion dhtVer;

        /** */
        private GridCacheVersion nearVer;

        /** Empty constructor required by {@link Externalizable}. */
        public TxInfo() {
            // No-op.
        }

        /**
         * @param dhtVer Tx dht version.
         * @param nearVer Tx near version.
         */
        TxInfo(GridCacheVersion dhtVer, GridCacheVersion nearVer) {
            this.dhtVer = dhtVer;
            this.nearVer = nearVer;
        }

        /** */
        @Override public short directType() {
            return -64;
        }

        /** {@inheritDoc} */
        @Override public void appendInfo(StringBuilder sb, GridKernalContext ctx) {
            sb.append(U.nl())
                .append("Related transactions [dhtVer=").append(dhtVer)
                .append(", nearVer=").append(nearVer).append("]: ");

            boolean found = false;

            for (IgniteInternalTx tx : ctx.cache().context().tm().activeTransactions()) {
                if (dhtVer.equals(tx.xidVersion()) || nearVer.equals(tx.nearXidVersion())) {
                    sb.append(U.nl())
                        .append("    ")
                        .append(tx.getClass().getSimpleName())
                        .append(" [ver=").append(tx.xidVersion())
                        .append(", nearVer=").append(tx.nearXidVersion())
                        .append(", topVer=").append(tx.topologyVersion())
                        .append(", state=").append(tx.state())
                        .append(", fullTx=").append(tx).append(']');

                    found = true;
                }
            }

            if (!found)
                sb.append(U.nl()).append("Failed to find related transactions.");
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(dhtVer);
            out.writeObject(nearVer);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            dhtVer = (GridCacheVersion)in.readObject();
            nearVer = (GridCacheVersion)in.readObject();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TxInfo txInfo = (TxInfo)o;

            return Objects.equals(dhtVer, txInfo.dhtVer) && Objects.equals(nearVer, txInfo.nearVer);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(getClass(), nearVer, dhtVer);
        }
    }

    /**
     * @param sb String builder.
     * @param ctx Context.
     */
    static void dumpNodeBasicInfo(StringBuilder sb, GridKernalContext ctx) {
        sb.append("General node info [id=").append(ctx.localNodeId())
            .append(", client=").append(ctx.clientNode())
            .append(", discoTopVer=").append(ctx.discovery().topologyVersionEx())
            .append(", time=").append(formatTime(U.currentTimeMillis())).append(']');
    }

    /**
     * @param sb String builder.
     * @param ctx Context.
     */
    static void dumpExchangeInfo(StringBuilder sb, GridKernalContext ctx) {
        GridCachePartitionExchangeManager<?, ?> exchMgr = ctx.cache().context().exchange();
        GridDhtTopologyFuture fut = exchMgr.lastTopologyFuture();

        sb.append("Partitions exchange info [readyVer=").append(exchMgr.readyAffinityVersion()).append(']').append(U.nl())
            .append("Last initialized exchange future: ").append(fut);
    }

    /**
     * @param sb String builder.
     * @param ctx Context.
     */
    static void dumpPendingCacheMessages(StringBuilder sb, GridKernalContext ctx) {
        ctx.cache().context().io().dumpPendingMessages(sb);
    }

    /**
     * @param ctx Context.
     * @param nodeId Target node ID.
     * @return Communication information future.
     */
    public static IgniteInternalFuture<String> dumpCommunicationInfo(GridKernalContext ctx, UUID nodeId) {
        if (ctx.config().getCommunicationSpi() instanceof TcpCommunicationSpi)
            return ((TcpCommunicationSpi)ctx.config().getCommunicationSpi()).dumpNodeStatistics(nodeId);
        else
            return new GridFinishedFuture<>("Unexpected communication SPI: " + ctx.config().getCommunicationSpi());
    }

    /**
     * @param time Time.
     * @return Time string.
     */
    private static String formatTime(long time) {
        return IgniteUtils.DEBUG_DATE_FMT.format(Instant.ofEpochMilli(time));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDiagnosticMessage.class, this);
    }
}
