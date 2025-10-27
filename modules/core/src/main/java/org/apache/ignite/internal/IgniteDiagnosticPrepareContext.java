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

import java.util.Collection;
import java.util.HashMap;
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

import static org.apache.ignite.internal.IgniteDiagnosticMessage.ExchangeInfo;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.TxEntriesInfo;
import static org.apache.ignite.internal.IgniteDiagnosticMessage.TxInfo;

/**
 * Groups diagnostic closures by node/closure type.
 */
public class IgniteDiagnosticPrepareContext {
    /** */
    private final UUID locNodeId;

    /** */
    private final Map<UUID, IgniteCompoundDiagnosicInfo> info = new HashMap<>();

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
    private IgniteCompoundDiagnosicInfo compoundInfo(UUID nodeId) {
        IgniteCompoundDiagnosicInfo compoundInfo = info.get(nodeId);

        if (compoundInfo == null)
            info.put(nodeId, compoundInfo = new IgniteCompoundDiagnosicInfo(locNodeId));

        return compoundInfo;
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
        for (Map.Entry<UUID, IgniteCompoundDiagnosicInfo> entry : info.entrySet()) {
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
}
