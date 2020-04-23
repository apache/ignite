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
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteDiagnosticMessage.DiagnosticBaseClosure;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.ExchangeInfoClosure;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.TxEntriesInfoClosure;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.TxInfoClosure;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.dumpCommunicationInfo;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.dumpExchangeInfo;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.dumpNodeBasicInfo;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.dumpPendingCacheMessages;

/**
 * Groups diagnostic closures by node/closure type.
 */
public class IgniteDiagnosticPrepareContext {
    /** */
    private final UUID locNodeId;

    /** */
    private final Map<UUID, CompoundInfoClosure> cls = new HashMap<>();

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
        closure(nodeId).add(msg, new ExchangeInfoClosure(topVer));
    }

    /**
     * @param nodeId Remote node ID.
     * @param cacheId Cache ID.
     * @param keys Entry keys.
     * @param msg Initial message.
     */
    public void txKeyInfo(UUID nodeId, int cacheId, Collection<KeyCacheObject> keys, String msg) {
        closure(nodeId).add(msg, new TxEntriesInfoClosure(cacheId, keys));
    }

    /**
     * @param nodeId Remote node ID.
     * @param dhtVer Tx dht version.
     * @param nearVer Tx near version.
     * @param msg Initial message.
     */
    public void remoteTxInfo(UUID nodeId, GridCacheVersion dhtVer, GridCacheVersion nearVer, String msg) {
        closure(nodeId).add(msg, new TxInfoClosure(dhtVer, nearVer));
    }

    /**
     * @param nodeId Remote node ID.
     * @param msg Initial message.
     */
    public void basicInfo(UUID nodeId, String msg) {
        closure(nodeId).add(msg, null);
    }

    /**
     * @param nodeId Remote node ID.
     * @return Compound closure
     */
    private CompoundInfoClosure closure(UUID nodeId) {
        CompoundInfoClosure cl = cls.get(nodeId);

        if (cl == null)
            cls.put(nodeId, cl = new CompoundInfoClosure(locNodeId));

        return cl;
    }

    /**
     * @return {@code True} if there are no added closures.
     */
    public boolean empty() {
        return cls.isEmpty();
    }

    /**
     * @param ctx Grid context.
     * @param lsnr Optional listener (used in test).
     */
    public void send(GridKernalContext ctx, @Nullable IgniteInClosure<IgniteInternalFuture<String>> lsnr) {
        for (Map.Entry<UUID, CompoundInfoClosure> entry : cls.entrySet()) {
            UUID rmtNodeId = entry.getKey();

            CompoundInfoClosure c = entry.getValue();

            IgniteInternalFuture<String> fut =
                ctx.cluster().requestDiagnosticInfo(rmtNodeId, c, c.message());

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
     *
     */
    private static final class CompoundInfoClosure implements IgniteClosure<GridKernalContext, IgniteDiagnosticInfo> {
        /** */
        private static final long serialVersionUID = 0L;

        /** ID of node sent closure. */
        protected final UUID nodeId;

        /** Closures to send on remote node. */
        private Map<Object, IgniteDiagnosticMessage.DiagnosticBaseClosure> cls = new LinkedHashMap<>();

        /** Local message related to remote closures. */
        private transient Map<Object, List<String>> msgs = new LinkedHashMap<>();

        /**
         * @param nodeId Node sent closure.
         */
        CompoundInfoClosure(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public final IgniteDiagnosticInfo apply(GridKernalContext ctx) {
            try {
                IgniteInternalFuture<String> commInfo = dumpCommunicationInfo(ctx, nodeId);

                StringBuilder sb = new StringBuilder();

                dumpNodeBasicInfo(sb, ctx);

                sb.append(U.nl());

                dumpExchangeInfo(sb, ctx);

                sb.append(U.nl());

                dumpPendingCacheMessages(sb, ctx);

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
            for (DiagnosticBaseClosure c : cls.values()) {
                try {
                    c.apply(sb, ctx);
                }
                catch (Exception e) {
                    ctx.cluster().diagnosticLog().error(
                        "Failed to populate diagnostic with additional information: " + e, e);

                    sb.append(U.nl()).append("Failed to populate diagnostic with additional information: ").append(e);
                }
            }
        }

        /**
         * @return Node ID.
         */
        public UUID nodeId() {
            return nodeId;
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
         * @param c Closure or {@code null} if only basic info is needed.
         */
        public void add(String msg, @Nullable DiagnosticBaseClosure c) {
            Object key = c != null ? c.mergeKey() : getClass();

            List<String> msgs0 = msgs.get(key);

            if (msgs0 == null) {
                msgs0 = new ArrayList<>();

                msgs.put(key, msgs0);
            }

            msgs0.add(msg);

            if (c != null) {
                DiagnosticBaseClosure c0 = cls.get(c.mergeKey());

                if (c0 == null)
                    cls.put(c.mergeKey(), c);
                else
                    c0.merge(c);
            }
        }
    }
}
