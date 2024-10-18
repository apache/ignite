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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.QueryEngineConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.ClientListenerAbstractConnectionContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestHandler;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponseSender;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxContext;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngineConfigurationEx;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.jdbc.thin.JdbcThinUtils.nullableBooleanFromByte;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.JDBC_CLIENT;

/**
 * JDBC Connection Context.
 */
public class JdbcConnectionContext extends ClientListenerAbstractConnectionContext {
    /** Version 2.1.0. */
    private static final ClientListenerProtocolVersion VER_2_1_0 = ClientListenerProtocolVersion.create(2, 1, 0);

    /** Version 2.1.5: added "lazy" flag. */
    private static final ClientListenerProtocolVersion VER_2_1_5 = ClientListenerProtocolVersion.create(2, 1, 5);

    /** Version 2.3.1: added "multiple statements query" feature. */
    static final ClientListenerProtocolVersion VER_2_3_0 = ClientListenerProtocolVersion.create(2, 3, 0);

    /** Version 2.4.0: adds default values for columns feature. */
    static final ClientListenerProtocolVersion VER_2_4_0 = ClientListenerProtocolVersion.create(2, 4, 0);

    /** Version 2.5.0: adds precision and scale for columns feature. */
    static final ClientListenerProtocolVersion VER_2_5_0 = ClientListenerProtocolVersion.create(2, 5, 0);

    /** Version 2.7.0: adds maximum length for columns feature.*/
    static final ClientListenerProtocolVersion VER_2_7_0 = ClientListenerProtocolVersion.create(2, 7, 0);

    /** Version 2.8.0: adds query id in order to implement cancel feature, partition awareness support: IEP-23.*/
    static final ClientListenerProtocolVersion VER_2_8_0 = ClientListenerProtocolVersion.create(2, 8, 0);

    /** Version 2.9.0: adds user attributes, adds features flags support. */
    static final ClientListenerProtocolVersion VER_2_9_0 = ClientListenerProtocolVersion.create(2, 9, 0);

    /** Version 2.13.0: adds choose of query engine support. */
    static final ClientListenerProtocolVersion VER_2_13_0 = ClientListenerProtocolVersion.create(2, 13, 0);

    /** Version 2.17.0: adds transaction default parameters. */
    static final ClientListenerProtocolVersion VER_2_17_0 = ClientListenerProtocolVersion.create(2, 17, 0);

    /** Current version. */
    public static final ClientListenerProtocolVersion CURRENT_VER = VER_2_17_0;

    /** Supported versions. */
    private static final Set<ClientListenerProtocolVersion> SUPPORTED_VERS = new HashSet<>();

    /** Default nested tx mode for compatibility. */
    public static final String DEFAULT_NESTED_TX_MODE = "ERROR";

    /** Shutdown busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Logger. */
    private final IgniteLogger log;

    /** Maximum allowed cursors. */
    private final int maxCursors;

    /** Message parser. */
    private JdbcMessageParser parser;

    /** Request handler. */
    private JdbcRequestHandler handler;

    /** Current protocol context. */
    private JdbcProtocolContext protoCtx;

    /** Last reported affinity topology version. */
    private AtomicReference<AffinityTopologyVersion> lastAffinityTopVer = new AtomicReference<>();

    /** Transaction context. */
    private @Nullable ClientTxContext txCtx;

    static {
        SUPPORTED_VERS.add(CURRENT_VER);
        SUPPORTED_VERS.add(VER_2_13_0);
        SUPPORTED_VERS.add(VER_2_9_0);
        SUPPORTED_VERS.add(VER_2_8_0);
        SUPPORTED_VERS.add(VER_2_7_0);
        SUPPORTED_VERS.add(VER_2_5_0);
        SUPPORTED_VERS.add(VER_2_4_0);
        SUPPORTED_VERS.add(VER_2_3_0);
        SUPPORTED_VERS.add(VER_2_1_5);
        SUPPORTED_VERS.add(VER_2_1_0);
    }

    /**
     * Constructor.
     * @param ctx Kernal Context.
     * @param ses Client's NIO session.
     * @param busyLock Shutdown busy lock.
     * @param connId Connection ID.
     * @param maxCursors Maximum allowed cursors.
     */
    public JdbcConnectionContext(GridKernalContext ctx, GridNioSession ses, GridSpinBusyLock busyLock, long connId,
        int maxCursors) {
        super(ctx, ses, connId);

        this.busyLock = busyLock;
        this.maxCursors = maxCursors;

        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public byte clientType() {
        return JDBC_CLIENT;
    }

    /** {@inheritDoc} */
    @Override public boolean isVersionSupported(ClientListenerProtocolVersion ver) {
        return SUPPORTED_VERS.contains(ver);
    }

    /** {@inheritDoc} */
    @Override public ClientListenerProtocolVersion defaultVersion() {
        return CURRENT_VER;
    }

    /** {@inheritDoc} */
    @Override public void initializeFromHandshake(GridNioSession ses,
        ClientListenerProtocolVersion ver, BinaryReaderExImpl reader)
        throws IgniteCheckedException {
        assert SUPPORTED_VERS.contains(ver) : "Unsupported JDBC protocol version.";

        boolean distributedJoins = reader.readBoolean();
        boolean enforceJoinOrder = reader.readBoolean();
        boolean collocated = reader.readBoolean();
        boolean replicatedOnly = reader.readBoolean();
        boolean autoCloseCursors = reader.readBoolean();

        boolean lazyExec = false;
        boolean skipReducerOnUpdate = false;
        String qryEngine = null;

        if (ver.compareTo(VER_2_1_5) >= 0)
            lazyExec = reader.readBoolean();

        if (ver.compareTo(VER_2_3_0) >= 0)
            skipReducerOnUpdate = reader.readBoolean();

        if (ver.compareTo(VER_2_7_0) >= 0) {
            String nestedTxModeName = reader.readString();

            if (!F.isEmpty(nestedTxModeName) && !nestedTxModeName.equals(DEFAULT_NESTED_TX_MODE))
                throw new IgniteCheckedException("Nested transactions are not supported!");
        }

        Boolean dataPageScanEnabled = null;
        Integer updateBatchSize = null;
        EnumSet<JdbcThinFeature> features = EnumSet.noneOf(JdbcThinFeature.class);

        if (ver.compareTo(VER_2_8_0) >= 0) {
            dataPageScanEnabled = nullableBooleanFromByte(reader.readByte());

            updateBatchSize = JdbcUtils.readNullableInteger(reader);
        }

        if (ver.compareTo(VER_2_9_0) >= 0) {
            userAttrs = reader.readMap();

            byte[] cliFeatures = reader.readByteArray();

            features = JdbcThinFeature.enumSet(cliFeatures);

            if (!U.isTxAwareQueriesEnabled(ctx))
                features.remove(JdbcThinFeature.TX_AWARE_QUERIES);
        }

        if (ver.compareTo(VER_2_13_0) >= 0) {
            qryEngine = reader.readString();

            if (qryEngine != null) {
                QueryEngineConfiguration[] cfgs = ctx.config().getSqlConfiguration().getQueryEnginesConfiguration();

                boolean found = false;

                if (cfgs != null) {
                    for (int i = 0; i < cfgs.length; i++) {
                        if (qryEngine.equalsIgnoreCase(((QueryEngineConfigurationEx)cfgs[i]).engineName())) {
                            found = true;
                            break;
                        }
                    }
                }

                if (!found)
                    throw new IgniteCheckedException("Not found configuration for query engine: " + qryEngine);
            }
        }

        TransactionConcurrency concurrency = null;
        TransactionIsolation isolation = null;
        int timeout = 0;
        String lb = null;

        if (ver.compareTo(VER_2_17_0) >= 0) {
            concurrency = TransactionConcurrency.fromOrdinal(reader.readByte());
            isolation = TransactionIsolation.fromOrdinal(reader.readByte());
            timeout = reader.readInt();
            lb = reader.readString();
        }

        if (ver.compareTo(VER_2_5_0) >= 0) {
            String user = null;
            String passwd = null;

            try {
                if (reader.available() > 0) {
                    user = reader.readString();
                    passwd = reader.readString();
                }
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Handshake error: " + e.getMessage(), e);
            }

            authenticate(ses, user, passwd);
        }

        protoCtx = new JdbcProtocolContext(ver, features, true);

        initClientDescriptor("jdbc-thin");

        parser = new JdbcMessageParser(ctx, protoCtx);

        ClientListenerResponseSender snd = new ClientListenerResponseSender() {
            @Override public void send(ClientListenerResponse resp) {
                if (resp != null) {
                    if (log.isDebugEnabled())
                        log.debug("Async response: [resp=" + resp.status() + ']');

                    ses.send(parser.encode(resp));
                }
            }
        };

        handler = new JdbcRequestHandler(busyLock, snd, maxCursors, distributedJoins, enforceJoinOrder,
            collocated, replicatedOnly, autoCloseCursors, lazyExec, skipReducerOnUpdate, qryEngine,
            dataPageScanEnabled, updateBatchSize,
            concurrency, isolation, timeout, lb,
            ver, this);

        handler.start();
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
        handler.onDisconnect();

        super.onDisconnected();
    }

    /** {@inheritDoc} */
    @Override public @Nullable ClientTxContext txContext(int txId) {
        ensureSameTransaction(txId);

        return txCtx;
    }

    /** {@inheritDoc} */
    @Override public void addTxContext(ClientTxContext txCtx) {
        if (this.txCtx != null)
            throw new IgniteSQLException("Too many transactions", IgniteQueryErrorCode.QUERY_CANCELED);

        this.txCtx = txCtx;
    }

    /** {@inheritDoc} */
    @Override public void removeTxContext(int txId) {
        ensureSameTransaction(txId);

        txCtx = null;
    }

    /** */
    private void ensureSameTransaction(int txId) {
        if (txCtx != null && txCtx.txId() != txId) {
            throw new IllegalStateException("Unknown transaction " +
                "[serverTxId=" + (txCtx == null ? null : txCtx.txId()) + ", txId=" + txId + ']');
        }
    }

    /** {@inheritDoc} */
    @Override protected void cleanupTxs() {
        if (txCtx != null)
            txCtx.close();

        txCtx = null;
    }

    /**
     * @return Retrieves current affinity topology version and sets it as a last if it was changed, false otherwise.
     */
    public AffinityTopologyVersion getAffinityTopologyVersionIfChanged() {
        while (true) {
            AffinityTopologyVersion oldVer = lastAffinityTopVer.get();
            AffinityTopologyVersion newVer = ctx.cache().context().exchange().readyAffinityVersion();

            boolean changed = oldVer == null || oldVer.compareTo(newVer) < 0;

            if (changed) {
                boolean success = lastAffinityTopVer.compareAndSet(oldVer, newVer);

                if (!success)
                    continue;
            }

            return changed ? newVer : null;
        }
    }

    /**
     * @return Binary context.
     */
    public JdbcProtocolContext protocolContext() {
        return protoCtx;
    }
}
