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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    /** Originator node id. */
    @Order(2)
    private UUID nodeId;

    /** Infos to send to a remote node. */
    @Order(3)
    private final Set<DiagnosticBaseInfo> infos = new LinkedHashSet<>();

    /** Local message related to remote info. */
    private final Map<Object, List<String>> msgs = new LinkedHashMap<>();

    /**
     * Default constructor required by {@link GridIoMessageFactory}.
     */
    public IgniteDiagnosticMessage() {
        // No-op.
    }

    /**
     * Creates a diagnostic info holder.
     *
     * @param nodeId Originator node ID.
     */
    IgniteDiagnosticMessage(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Creates a diagnostic request.
     *
     * @param futId Future ID.
     * @param nodeId Node ID.
     * @param infos Diagnostic infos.
     */
    public IgniteDiagnosticMessage(long futId, UUID nodeId, Collection<DiagnosticBaseInfo> infos) {
        this(nodeId);

        this.futId = futId;
        this.infos.addAll(infos);
    }

    /**
     * Creates a diagnostic response.
     *
     * @param resp Diagnostic info result.
     * @param futId Future ID.
     */
    public IgniteDiagnosticMessage(String resp, long futId) {
        this.futId = futId;
        infoResp = resp;
    }

    /**
     * @param ctx Grid context.
     * @return Diagnostic info.
     */
    public String diagnosticInfo(GridKernalContext ctx) {
        try {
            IgniteInternalFuture<String> commInfo = dumpCommunicationInfo(ctx, nodeId);

            StringBuilder sb = new StringBuilder();

            dumpNodeBasicInfo(sb, ctx);

            sb.append(U.nl());

            dumpExchangeInfo(sb, ctx);

            sb.append(U.nl());

            ctx.cache().context().io().dumpPendingMessages(sb);

            sb.append(commInfo.get(10_000));

            moreInfo(sb, ctx);

            return sb.toString();
        }
        catch (Exception e) {
            ctx.cluster().diagnosticLog().error("Failed to execute diagnostic message closure: " + e, e);

            return "Failed to execute diagnostic message closure: " + e;
        }
    }

    /**
     * @param sb String builder.
     * @param ctx Grid context.
     */
    private void moreInfo(StringBuilder sb, GridKernalContext ctx) {
        for (IgniteDiagnosticMessage.DiagnosticBaseInfo baseInfo : infos) {
            try {
                baseInfo.appendInfo(sb, ctx);
            }
            catch (Exception e) {
                ctx.cluster().diagnosticLog().error(
                    "Failed to populate diagnostic with additional information: " + e, e);

                sb.append(U.nl()).append("Failed to populate diagnostic with additional information: ").append(e);
            }
        }
    }

    /**
     * @return Initial message.
     */
    public String message() {
        StringBuilder sb = new StringBuilder();

        for (List<String> msgs0 : msgs.values()) {
            for (String msg : msgs0) {
                if (sb.length() > 0)
                    sb.append('\n');

                sb.append(msg);
            }
        }

        return sb.toString();
    }

    /**
     * @param msg Message.
     * @param baseInfo Info or {@code null} if only basic info is needed.
     */
    void add(String msg, @Nullable IgniteDiagnosticMessage.DiagnosticBaseInfo baseInfo) {
        Object key = baseInfo != null ? baseInfo : getClass();

        msgs.computeIfAbsent(key, k -> new ArrayList<>()).add(msg);

        if (baseInfo != null) {
            if (!infos.add(baseInfo) && baseInfo instanceof TxEntriesInfo) {
                for (IgniteDiagnosticMessage.DiagnosticBaseInfo baseInfo0 : infos) {
                    if (baseInfo0.equals(baseInfo))
                        baseInfo0.merge(baseInfo);
                }
            }
        }
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
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

    /** @return Compound diagnostic infos.  */
    public Collection<DiagnosticBaseInfo> infos() {
        return Collections.unmodifiableCollection(infos);
    }

    /** */
    public void infos(Collection<DiagnosticBaseInfo> infos) {
        this.infos.clear();
        this.infos.addAll(infos);
    }

    /** */
    public @Nullable String infoResponse() {
        return infoResp;
    }

    /** */
    public void infoResponse(@Nullable String infoResp) {
        this.infoResp = infoResp;
    }


    /** {@inheritDoc} */
    @Override public short directType() {
        return -61;
    }

    /**
     * @param sb String builder.
     * @param ctx Context.
     */
    private static void dumpNodeBasicInfo(StringBuilder sb, GridKernalContext ctx) {
        sb.append("General node info [id=").append(ctx.localNodeId())
            .append(", client=").append(ctx.clientNode())
            .append(", discoTopVer=").append(ctx.discovery().topologyVersionEx())
            .append(", time=").append(IgniteUtils.DEBUG_DATE_FMT.format(Instant.ofEpochMilli(U.currentTimeMillis()))).append(']');
    }

    /**
     * @param sb String builder.
     * @param ctx Context.
     */
    private static void dumpExchangeInfo(StringBuilder sb, GridKernalContext ctx) {
        GridCachePartitionExchangeManager<?, ?> exchMgr = ctx.cache().context().exchange();
        GridDhtTopologyFuture fut = exchMgr.lastTopologyFuture();

        sb.append("Partitions exchange info [readyVer=").append(exchMgr.readyAffinityVersion()).append(']').append(U.nl())
            .append("Last initialized exchange future: ").append(fut);
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDiagnosticMessage.class, this);
    }

    /** */
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
}
