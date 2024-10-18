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

package org.apache.ignite.internal.processors.platform.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerAbstractConnectionContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestHandler;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxContext;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.THIN_CLIENT;
import static org.apache.ignite.internal.processors.platform.client.ClientBitmaskFeature.USER_ATTRIBUTES;
import static org.apache.ignite.internal.processors.platform.client.ClientProtocolVersionFeature.AUTHORIZATION;
import static org.apache.ignite.internal.processors.platform.client.ClientProtocolVersionFeature.BITMAP_FEATURES;

/**
 * Thin Client connection context.
 */
public class ClientConnectionContext extends ClientListenerAbstractConnectionContext {
    /** Version 1.0.0. */
    public static final ClientListenerProtocolVersion VER_1_0_0 = ClientListenerProtocolVersion.create(1, 0, 0);

    /** Version 1.1.0. */
    public static final ClientListenerProtocolVersion VER_1_1_0 = ClientListenerProtocolVersion.create(1, 1, 0);

    /** Version 1.2.0. */
    public static final ClientListenerProtocolVersion VER_1_2_0 = ClientListenerProtocolVersion.create(1, 2, 0);

    /** Version 1.3.0. */
    public static final ClientListenerProtocolVersion VER_1_3_0 = ClientListenerProtocolVersion.create(1, 3, 0);

    /** Version 1.4.0. Added: Partition awareness, IEP-23. */
    public static final ClientListenerProtocolVersion VER_1_4_0 = ClientListenerProtocolVersion.create(1, 4, 0);

    /** Version 1.5.0. Added: Transactions support, IEP-34. */
    public static final ClientListenerProtocolVersion VER_1_5_0 = ClientListenerProtocolVersion.create(1, 5, 0);

    /** Version 1.6.0. Added: Expiration Policy configuration. */
    public static final ClientListenerProtocolVersion VER_1_6_0 = ClientListenerProtocolVersion.create(1, 6, 0);

    /**
     * Version 1.7.0. Added: protocol features.
     * ATTENTION! Do not add any new protocol versions unless totally necessary. Use {@link ClientBitmaskFeature}
     * instead.
     */
    public static final ClientListenerProtocolVersion VER_1_7_0 = ClientListenerProtocolVersion.create(1, 7, 0);

    /** Default version. */
    public static final ClientListenerProtocolVersion DEFAULT_VER = VER_1_7_0;

    /** Supported versions. */
    private static final Collection<ClientListenerProtocolVersion> SUPPORTED_VERS = Arrays.asList(
        VER_1_7_0,
        VER_1_6_0,
        VER_1_5_0,
        VER_1_4_0,
        VER_1_3_0,
        VER_1_2_0,
        VER_1_1_0,
        VER_1_0_0
    );

    /** Message parser. */
    private ClientMessageParser parser;

    /** Request handler. */
    private ClientRequestHandler handler;

    /** Handle registry. */
    private final ClientResourceRegistry resReg = new ClientResourceRegistry();

    /** Max cursors. */
    private final int maxCursors;

    /** Current protocol context. */
    private ClientProtocolContext currentProtocolContext;

    /** Last reported affinity topology version. */
    private AtomicReference<AffinityTopologyVersion> lastAffinityTopologyVersion = new AtomicReference<>();

    /** Client session. */
    private GridNioSession ses;

    /** Cursor counter. */
    private final AtomicLong curCnt = new AtomicLong();

    /** Active tx count limit. */
    private final int maxActiveTxCnt;

    /** Transactions by transaction id. */
    private final Map<Integer, ClientTxContext> txs = new ConcurrentHashMap<>();

    /** Active transactions count. */
    private final AtomicInteger txsCnt = new AtomicInteger();

    /** Active compute tasks limit. */
    private final int maxActiveComputeTasks;

    /** Active compute tasks count. */
    private final AtomicInteger activeTasksCnt = new AtomicInteger();

    /**
     * Ctor.
     *
     * @param ctx Kernal context.
     * @param connId Connection ID.
     * @param maxCursors Max active cursors.
     * @param thinCfg Thin-client configuration.
     */
    public ClientConnectionContext(
        GridKernalContext ctx,
        GridNioSession ses,
        long connId,
        int maxCursors,
        ThinClientConfiguration thinCfg
    ) {
        super(ctx, ses, connId);

        this.maxCursors = maxCursors;
        maxActiveTxCnt = thinCfg.getMaxActiveTxPerConnection();
        maxActiveComputeTasks = thinCfg.getMaxActiveComputeTasksPerConnection();
    }

    /**
     * Gets the handle registry.
     *
     * @return Handle registry.
     */
    public ClientResourceRegistry resources() {
        return resReg;
    }

    /** {@inheritDoc} */
    @Override public byte clientType() {
        return THIN_CLIENT;
    }

    /** {@inheritDoc} */
    @Override public boolean isVersionSupported(ClientListenerProtocolVersion ver) {
        return SUPPORTED_VERS.contains(ver);
    }

    /** {@inheritDoc} */
    @Override public ClientListenerProtocolVersion defaultVersion() {
        return DEFAULT_VER;
    }

    /**
     * @return Currently used protocol context.
     */
    public ClientProtocolContext currentProtocolContext() {
        return currentProtocolContext;
    }

    /** {@inheritDoc} */
    @Override public void initializeFromHandshake(GridNioSession ses,
        ClientListenerProtocolVersion ver, BinaryReaderExImpl reader)
        throws IgniteCheckedException {

        EnumSet<ClientBitmaskFeature> features = null;

        if (ClientProtocolContext.isFeatureSupported(ver, BITMAP_FEATURES)) {
            byte[] cliFeatures = reader.readByteArray();

            features = ClientBitmaskFeature.enumSet(cliFeatures);

            if (!U.isTxAwareQueriesEnabled(ctx))
                features.remove(ClientBitmaskFeature.TX_AWARE_QUERIES);
        }

        currentProtocolContext = new ClientProtocolContext(ver, features);

        String user = null;
        String pwd = null;

        if (currentProtocolContext.isFeatureSupported(USER_ATTRIBUTES))
            userAttrs = reader.readMap();

        if (currentProtocolContext.isFeatureSupported(AUTHORIZATION)) {
            boolean hasMore;
            try {
                hasMore = reader.available() > 0;
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Handshake error: " + e.getMessage(), e);
            }

            if (hasMore) {
                user = reader.readString();
                pwd = reader.readString();
            }
        }

        authenticate(ses, user, pwd);

        initClientDescriptor("cli");

        handler = new ClientRequestHandler(this, currentProtocolContext);
        parser = new ClientMessageParser(this, currentProtocolContext);

        this.ses = ses;
    }

    /** {@inheritDoc} */
    @Override public ClientListenerRequestHandler handler() {
        return handler;
    }

    /** {@inheritDoc} */
    @Override public ClientListenerMessageParser parser() {
        return parser;
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected() {
        resReg.clean();

        super.onDisconnected();
    }

    /**
     * Increments the cursor count.
     */
    public void incrementCursors() {
        long curCnt0 = curCnt.get();

        if (curCnt0 >= maxCursors) {
            throw new IgniteClientException(ClientStatus.TOO_MANY_CURSORS,
                "Too many open cursors (either close other open cursors or increase the " +
                "limit through ClientConnectorConfiguration.maxOpenCursorsPerConnection) [maximum=" + maxCursors +
                ", current=" + curCnt0 + ']');
        }

        curCnt.incrementAndGet();
    }

    /**
     * Increments the cursor count.
     */
    public void decrementCursors() {
        curCnt.decrementAndGet();
    }

    /**
     * Atomically check whether affinity topology version has changed since the last call and sets new version as a last.
     * @return New version, if it has changed since the last call.
     */
    public ClientAffinityTopologyVersion checkAffinityTopologyVersion() {
        while (true) {
            AffinityTopologyVersion oldVer = lastAffinityTopologyVersion.get();
            AffinityTopologyVersion newVer = ctx.cache().context().exchange().readyAffinityVersion();

            boolean changed = oldVer == null || oldVer.compareTo(newVer) < 0;

            if (changed) {
                boolean success = lastAffinityTopologyVersion.compareAndSet(oldVer, newVer);

                if (!success)
                    continue;
            }

            return new ClientAffinityTopologyVersion(newVer, changed);
        }
    }

    /** {@inheritDoc} */
    @Override public @Nullable ClientTxContext txContext(int txId) {
        return txs.get(txId);
    }

    /** {@inheritDoc} */
    @Override public void addTxContext(ClientTxContext txCtx) {
        if (txsCnt.incrementAndGet() > maxActiveTxCnt) {
            txsCnt.decrementAndGet();

            throw new IgniteClientException(ClientStatus.TX_LIMIT_EXCEEDED, "Active transactions per connection limit " +
                "(" + maxActiveTxCnt + ") exceeded. To start a new transaction you need to wait for some of currently " +
                "active transactions complete. To change the limit set up " +
                "ThinClientConfiguration.MaxActiveTxPerConnection property.");
        }

        txs.put(txCtx.txId(), txCtx);
    }

    /** {@inheritDoc} */
    @Override public void removeTxContext(int txId) {
        txs.remove(txId);

        txsCnt.decrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void cleanupTxs() {
        for (ClientTxContext txCtx : txs.values())
            txCtx.close();

        txs.clear();
    }

    /**
     * Send notification to the client.
     *
     * @param notification Notification.
     */
    public void notifyClient(ClientNotification notification) {
        ses.send(parser.encode(notification));
    }

    /**
     * Increments the active compute tasks count.
     */
    public void incrementActiveTasksCount() {
        if (maxActiveComputeTasks == 0) {
            throw new IgniteClientException(ClientStatus.FUNCTIONALITY_DISABLED,
                "Compute grid functionality is disabled for thin clients on server node. " +
                    "To enable it set up the ThinClientConfiguration.MaxActiveComputeTasksPerConnection property.");
        }

        if (activeTasksCnt.incrementAndGet() > maxActiveComputeTasks) {
            activeTasksCnt.decrementAndGet();

            throw new IgniteClientException(ClientStatus.TOO_MANY_COMPUTE_TASKS, "Active compute tasks per connection " +
                "limit (" + maxActiveComputeTasks + ") exceeded. To start a new task you need to wait for some of " +
                "currently active tasks complete. To change the limit set up the " +
                "ThinClientConfiguration.MaxActiveComputeTasksPerConnection property.");
        }
    }

    /**
     * Decrements the active compute tasks count.
     */
    public void decrementActiveTasksCount() {
        int cnt = activeTasksCnt.decrementAndGet();

        assert cnt >= 0 : "Unexpected active tasks count: " + cnt;
    }
}
