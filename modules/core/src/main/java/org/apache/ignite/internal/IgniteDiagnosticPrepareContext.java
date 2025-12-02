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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Groups diagnostic messages by node/closure type.
 */
public class IgniteDiagnosticPrepareContext {
    /** */
    private final UUID locNodeId;

    /** */
    private final Map<UUID, IgniteDiagnosticRequest> info = new HashMap<>();

    /**
     * @param nodeId Local node ID.
     */
    public IgniteDiagnosticPrepareContext(UUID nodeId) {
        locNodeId = nodeId;
    }

    /**
     * @param nodeId Remote node ID.
     * @param topVer Topology version.
     * @param msg Initial message.
     */
    public void exchangeInfo(UUID nodeId, AffinityTopologyVersion topVer, String msg) {
        diagnosticInfo(nodeId).add(msg, new ExchangeInfo(topVer));
    }

    /**
     * @param nodeId Remote node ID.
     * @param cacheId Cache ID.
     * @param keys Entry keys.
     * @param msg Initial message.
     */
    public void txKeyInfo(UUID nodeId, int cacheId, Collection<KeyCacheObject> keys, String msg) {
        diagnosticInfo(nodeId).add(msg, new TxEntriesInfo(cacheId, keys));
    }

    /**
     * @param nodeId Remote node ID.
     * @param dhtVer Tx dht version.
     * @param nearVer Tx near version.
     * @param msg Initial message.
     */
    public void remoteTxInfo(UUID nodeId, GridCacheVersion dhtVer, GridCacheVersion nearVer, String msg) {
        diagnosticInfo(nodeId).add(msg, new TxInfo(dhtVer, nearVer));
    }

    /**
     * @param nodeId Remote node ID.
     * @param msg Initial message.
     */
    public void basicInfo(UUID nodeId, String msg) {
        diagnosticInfo(nodeId).add(msg, null);
    }

    /**
     * @param nodeId Remote node ID.
     * @return Compound info.
     */
    private IgniteDiagnosticRequest diagnosticInfo(UUID nodeId) {
        return info.computeIfAbsent(nodeId, nid -> new IgniteDiagnosticRequest(locNodeId));
    }

    /**
     * @return {@code True} if there are no added info.
     */
    public boolean empty() {
        return info.isEmpty();
    }

    /**
     * @param ctx Grid context.
     * @param lsnr Optional listener (used in test).
     */
    public void send(GridKernalContext ctx, @Nullable IgniteInClosure<IgniteInternalFuture<String>> lsnr) {
        for (Map.Entry<UUID, IgniteDiagnosticRequest> entry : info.entrySet()) {
            IgniteInternalFuture<String> fut = ctx.cluster().requestDiagnosticInfo(entry.getKey(), entry.getValue());

            if (lsnr != null)
                fut.listen(lsnr);

            listenAndLog(ctx.cluster().diagnosticLog(), fut);
        }
    }

    /**
     * @param log Logger.
     * @param fut Future.
     */
    private void listenAndLog(final IgniteLogger log, IgniteInternalFuture<String> fut) {
        fut.listen(new CI1<IgniteInternalFuture<String>>() {
            @Override public void apply(IgniteInternalFuture<String> fut) {
                synchronized (IgniteDiagnosticPrepareContext.class) {
                    try {
                        if (log.isInfoEnabled())
                            log.info(fut.get());
                    }
                    catch (Exception e) {
                        U.error(log, "Failed to dump diagnostic info: " + e, e);
                    }
                }
            }
        });
    }

    /**
     * @param ctx Grid context.
     * @param req Diagnostic info request.
     * @return Diagnostic info response.
     */
    public static IgniteDiagnosticResponse diagnosticInfoResponse(GridKernalContext ctx, IgniteDiagnosticRequest req) {
        try {
            IgniteInternalFuture<String> commInfo = dumpCommunicationInfo(ctx, req.nodeId());

            StringBuilder sb = new StringBuilder();

            dumpNodeBasicInfo(sb, ctx);

            sb.append(U.nl());

            dumpExchangeInfo(sb, ctx);

            sb.append(U.nl());

            ctx.cache().context().io().dumpPendingMessages(sb);

            sb.append(commInfo.get(10_000));

            moreInfo(sb, ctx, req.infos());

            return new IgniteDiagnosticResponse(req.futureId(), sb.toString());
        }
        catch (Exception e) {
            ctx.cluster().diagnosticLog().error("Failed to execute diagnostic message closure: " + e, e);

            return new IgniteDiagnosticResponse(req.futureId(), "Failed to execute diagnostic message closure: " + e);
        }
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

    /**
     * @param sb String builder.
     * @param ctx Grid context.
     * @param info Collection of the infos.
     */
    private static void moreInfo(StringBuilder sb, GridKernalContext ctx, Collection<IgniteDiagnosticRequest.DiagnosticBaseInfo> info) {
        for (IgniteDiagnosticRequest.DiagnosticBaseInfo baseInfo : info) {
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
}
