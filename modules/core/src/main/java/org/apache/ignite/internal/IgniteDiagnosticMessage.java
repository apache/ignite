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

import java.time.Instant;
import java.util.UUID;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
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
     * Creates diagnostic request.
     *
     * @param info Compound info.
     * @param futId Future ID.
     */
    public IgniteDiagnosticMessage(IgniteCompoundDiagnosicInfo info, long futId) {
        this.futId = futId;
        compoundInfo = info;
    }

    /**
     * Creates diagnostic response.
     *
     * @param resp Diagnostic info response.
     * @param futId Future ID.
     */
    public IgniteDiagnosticMessage(String resp, long futId) {
        this.futId = futId;
        infoResp = resp;
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
    public abstract static class DiagnosticBaseInfo implements Message {
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
