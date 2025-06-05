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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteDiagnosticMessage.DiagnosticBaseInfo;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.ExchangeInfo;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.TxEntriesInfo;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.TxInfo;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.dumpCommunicationInfo;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.dumpExchangeInfo;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.dumpNodeBasicInfo;

/** Groups diagnostic info by node/info type. */
public class IgniteDiagnosticPrepareContext {
    /** */
    private final UUID locNodeId;

    /** */
    private final Map<UUID, CompoundInfo> info = new HashMap<>();

    /** @param nodeId Local node ID. */
    public IgniteDiagnosticPrepareContext(UUID nodeId) {
        locNodeId = nodeId;
    }

    /**
     * @param nodeId Remote node ID.
     * @param topVer Topology version.
     * @param msg Initial message.
     */
    public void exchangeInfo(UUID nodeId, AffinityTopologyVersion topVer, String msg) {
        compoundInfo(nodeId).add(msg, new ExchangeInfo(topVer));
    }

    /**
     * @param nodeId Remote node ID.
     * @param cacheId Cache ID.
     * @param keys Entry keys.
     * @param msg Initial message.
     */
    public void txKeyInfo(UUID nodeId, int cacheId, Collection<KeyCacheObject> keys, String msg) {
        compoundInfo(nodeId).add(msg, new TxEntriesInfo(cacheId, keys));
    }

    /**
     * @param nodeId Remote node ID.
     * @param dhtVer Tx dht version.
     * @param nearVer Tx near version.
     * @param msg Initial message.
     */
    public void remoteTxInfo(UUID nodeId, GridCacheVersion dhtVer, GridCacheVersion nearVer, String msg) {
        compoundInfo(nodeId).add(msg, new TxInfo(dhtVer, nearVer));
    }

    /**
     * @param nodeId Remote node ID.
     * @param msg Initial message.
     */
    public void basicInfo(UUID nodeId, String msg) {
        compoundInfo(nodeId).add(msg, null);
    }

    /**
     * @param nodeId Remote node ID.
     * @return Compound info.
     */
    private CompoundInfo compoundInfo(UUID nodeId) {
        CompoundInfo i = info.get(nodeId);

        if (i == null)
            info.put(nodeId, i = new CompoundInfo(locNodeId));

        return i;
    }

    /** @return {@code True} if there are no added info. */
    public boolean empty() {
        return info.isEmpty();
    }

    /**
     * @param ctx Grid context.
     * @param lsnr Optional listener (used in test).
     */
    public void send(GridKernalContext ctx, @Nullable IgniteInClosure<IgniteInternalFuture<String>> lsnr) {
        for (Map.Entry<UUID, CompoundInfo> entry : info.entrySet()) {
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

    /** */
    public static final class CompoundInfo implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** ID of node sent info. */
        private final UUID nodeId;

        /** Info to send on remote node. */
        private Map<Object, DiagnosticBaseInfo> info = new LinkedHashMap<>();

        /** Local message related to remote info. */
        private transient Map<Object, List<String>> msgs = new LinkedHashMap<>();

        /** @param nodeId ID of node sent info. */
        CompoundInfo(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * @param ctx Grid context.
         * @return Diagnostic info.
         */
        public final IgniteDiagnosticInfo diagnosticInfo(GridKernalContext ctx) {
            try {
                IgniteInternalFuture<String> commInfo = dumpCommunicationInfo(ctx.config().getCommunicationSpi(), nodeId);

                StringBuilder sb = new StringBuilder();

                dumpNodeBasicInfo(sb, ctx);

                sb.append(U.nl());

                dumpExchangeInfo(sb, ctx.cache().context().exchange());

                sb.append(U.nl());

                ctx.cache().context().io().dumpPendingMessages(sb);

                sb.append(commInfo.get(10_000));

                moreInfo(sb, ctx);

                return new IgniteDiagnosticInfo(sb.toString());
            }
            catch (Exception e) {
                ctx.cluster().diagnosticLog().error("Failed to execute diagnostic message closure: " + e, e);

                return new IgniteDiagnosticInfo("Failed to execute diagnostic message closure: " + e);
            }
        }

        /**
         * @param sb String builder.
         * @param ctx Grid context.
         */
        private void moreInfo(StringBuilder sb, GridKernalContext ctx) {
            for (DiagnosticBaseInfo i : info.values()) {
                try {
                    i.appendInfo(sb, ctx);
                }
                catch (Exception e) {
                    ctx.cluster().diagnosticLog().error(
                            "Failed to populate diagnostic with additional information: " + e, e);

                    sb.append(U.nl()).append("Failed to populate diagnostic with additional information: ").append(e);
                }
            }
        }

        /** @return Initial message. */
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
         * @param i Addition info or {@code null} if only basic info is needed.
         */
        public void add(String msg, @Nullable IgniteDiagnosticMessage.DiagnosticBaseInfo i) {
            Object key = i != null ? i.mergeKey() : getClass();

            List<String> msgs0 = msgs.computeIfAbsent(key, k -> new ArrayList<>());

            msgs0.add(msg);

            if (i != null) {
                DiagnosticBaseInfo i0 = info.get(i.mergeKey());

                if (i0 == null)
                    info.put(i.mergeKey(), i);
                else
                    i0.merge(i);
            }
        }
    }
}
