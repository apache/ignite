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

package org.apache.ignite.internal.processors.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.query.CacheQueryFuture;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorImpl;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexOperationCancellationToken;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationClientFuture;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationManager;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationWorker;
import org.apache.ignite.internal.processors.query.schema.message.SchemaAbstractDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.message.SchemaOperationStatusMessage;
import org.apache.ignite.internal.processors.query.schema.message.SchemaProposeDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAbstractOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexCreateOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexDropOperation;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerFuture;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.internal.GridTopic.TOPIC_SCHEMA;
import static org.apache.ignite.internal.IgniteComponentType.INDEXING;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SCHEMA_POOL;

/**
 * Indexing processor.
 */
public class GridQueryProcessor extends GridProcessorAdapter {
    /** Queries detail metrics eviction frequency. */
    private static final int QRY_DETAIL_METRICS_EVICTION_FREQ = 3_000;

    /** */
    private static final ThreadLocal<AffinityTopologyVersion> requestTopVer = new ThreadLocal<>();

    /** For tests. */
    public static Class<? extends GridQueryIndexing> idxCls;

    /** JDK marshaller to serialize errors. */
    private final JdkMarshaller marsh = new JdkMarshaller();

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** */
    private GridTimeoutProcessor.CancelableTask qryDetailMetricsEvictTask;

    /** Type descriptors. */
    private final Map<QueryTypeIdKey, QueryTypeDescriptorImpl> types = new ConcurrentHashMap<>();

    /** Type descriptors. */
    private final ConcurrentMap<QueryTypeNameKey, QueryTypeDescriptorImpl> typesByName = new ConcurrentHashMap<>();

    /** */
    private final GridQueryIndexing idx;

    /** Value object context. */
    private final CacheQueryObjectValueContext valCtx;

    /** All indexes. */
    private final ConcurrentMap<QueryIndexKey, QueryIndexDescriptorImpl> idxs = new ConcurrentHashMap<>();

    /** Schema operation futures created on client side. */
    private final ConcurrentMap<UUID, SchemaOperationClientFuture> schemaCliFuts = new ConcurrentHashMap<>();

    /** IO message listener. */
    private final GridMessageListener ioLsnr;

    /** Schema operations. */
    private final ConcurrentHashMap<String, SchemaOperation> schemaOps = new ConcurrentHashMap<>();

    /** Active propose messages. */
    private final LinkedHashMap<UUID, SchemaProposeDiscoveryMessage> activeProposals = new LinkedHashMap<>();

    /** General state mutex. */
    private final Object stateMux = new Object();

    /** Coordinator node (initialized lazily). */
    private ClusterNode crd;

    /** Registered cache names. */
    private final Collection<String> cacheNames = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /** ID history for index create/drop discovery messages. */
    private final GridBoundedConcurrentLinkedHashSet<IgniteUuid> dscoMsgIdHist =
        new GridBoundedConcurrentLinkedHashSet<>(QueryUtils.discoveryHistorySize());

    /** History of already completed operations. */
    private final GridBoundedConcurrentLinkedHashSet<UUID> completedOpIds =
        new GridBoundedConcurrentLinkedHashSet<>(QueryUtils.discoveryHistorySize());

    /** Pending status messages. */
    private final LinkedList<SchemaOperationStatusMessage> pendingMsgs = new LinkedList<>();

    /** Disconnected flag. */
    private boolean disconnected;

    /** Whether exchange thread is ready to process further requests. */
    private boolean exchangeReady;

    /** */
    private boolean skipFieldLookup;

    /**
     * @param ctx Kernal context.
     */
    public GridQueryProcessor(GridKernalContext ctx) throws IgniteCheckedException {
        super(ctx);

        if (idxCls != null) {
            idx = U.newInstance(idxCls);

            idxCls = null;
        }
        else
            idx = INDEXING.inClassPath() ? U.<GridQueryIndexing>newInstance(INDEXING.className()) : null;

        valCtx = new CacheQueryObjectValueContext(ctx);

        ioLsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                if (msg instanceof SchemaOperationStatusMessage) {
                    SchemaOperationStatusMessage msg0 = (SchemaOperationStatusMessage)msg;

                    msg0.senderNodeId(nodeId);

                    processStatusMessage(msg0);
                }
                else
                    U.warn(log, "Unsupported IO message: " + msg);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void start(boolean activeOnStart) throws IgniteCheckedException {
        super.start(activeOnStart);

        if (idx != null) {
            ctx.resource().injectGeneric(idx);

            idx.start(ctx, busyLock);
        }

        ctx.io().addMessageListener(TOPIC_SCHEMA, ioLsnr);

        // Schedule queries detail metrics eviction.
        qryDetailMetricsEvictTask = ctx.timeout().schedule(new Runnable() {
            @Override public void run() {
                for (IgniteCacheProxy cache : ctx.cache().jcaches())
                    cache.context().queries().evictDetailMetrics();
            }
        }, QRY_DETAIL_METRICS_EVICTION_FREQ, QRY_DETAIL_METRICS_EVICTION_FREQ);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        if (cancel && idx != null) {
            try {
                while (!busyLock.tryBlock(500))
                    idx.cancelAllQueries();

                return;
            } catch (InterruptedException ignored) {
                U.warn(log, "Interrupted while waiting for active queries cancellation.");

                Thread.currentThread().interrupt();
            }
        }

        busyLock.block();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        ctx.io().removeMessageListener(TOPIC_SCHEMA, ioLsnr);

        if (idx != null)
            idx.stop();

        U.closeQuiet(qryDetailMetricsEvictTask);
    }

    /**
     * Handle cache kernal start. At this point discovery and IO managers are operational, caches are not started yet.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheKernalStart() throws IgniteCheckedException {
        synchronized (stateMux) {
            exchangeReady = true;

            // Re-run pending top-level proposals.
            for (SchemaOperation schemaOp : schemaOps.values())
                onSchemaPropose(schemaOp.proposeMessage());
        }
    }

    /**
     * Handle cache reconnect.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheReconnect() throws IgniteCheckedException {
        synchronized (stateMux) {
            assert disconnected;

            disconnected = false;

            onCacheKernalStart();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return DiscoveryDataExchangeType.QUERY_PROC;
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        // Collect active proposals.
        synchronized (stateMux) {
            LinkedHashMap<UUID, SchemaProposeDiscoveryMessage> data = new LinkedHashMap<>(activeProposals);

            dataBag.addGridCommonData(DiscoveryDataExchangeType.QUERY_PROC.ordinal(), data);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        synchronized (stateMux) {
            // Preserve proposals.
            LinkedHashMap<UUID, SchemaProposeDiscoveryMessage> data0 =
                (LinkedHashMap<UUID, SchemaProposeDiscoveryMessage>)data.commonData();

            // Process proposals as if they were received as regular discovery messages.
            if (data0 != null) {
                for (SchemaProposeDiscoveryMessage activeProposal : data0.values())
                    onSchemaProposeDiscovery0(activeProposal);
            }
        }
    }

    /**
     * Process schema propose message from discovery thread.
     *
     * @param msg Message.
     * @return {@code True} if exchange should be triggered.
     */
    private boolean onSchemaProposeDiscovery(SchemaProposeDiscoveryMessage msg) {
        SchemaAbstractOperation op = msg.operation();

        UUID opId = op.id();
        String cacheName = op.cacheName();

        if (!msg.initialized()) {
            // Ensure cache exists on coordinator node.
            DynamicCacheDescriptor cacheDesc = ctx.cache().cacheDescriptor(cacheName);

            if (cacheDesc == null) {
                if (log.isDebugEnabled())
                    log.debug("Received schema propose discovery message, but cache doesn't exist " +
                        "(will report error) [opId=" + opId + ", msg=" + msg + ']');

                msg.onError(new SchemaOperationException(SchemaOperationException.CODE_CACHE_NOT_FOUND, cacheName));
            }
            else {
                CacheConfiguration ccfg = cacheDesc.cacheConfiguration();

                if (ccfg.getCacheMode() == CacheMode.LOCAL) {
                    // Distributed operation is not allowed on LOCAL caches.
                    if (log.isDebugEnabled())
                        log.debug("Received schema propose discovery message, but cache is LOCAL " +
                            "(will report error) [opId=" + opId + ", msg=" + msg + ']');

                    msg.onError(new SchemaOperationException("Schema changes are not supported for LOCAL cache."));
                }
                else {
                    // Preserve deployment ID so that we can distinguish between different caches with the same name.
                    if (msg.deploymentId() == null)
                        msg.deploymentId(cacheDesc.deploymentId());

                    assert F.eq(cacheDesc.deploymentId(), msg.deploymentId());
                }
            }
        }

        // Complete client future and exit immediately in case of error.
        if (msg.hasError()) {
            SchemaOperationClientFuture cliFut = schemaCliFuts.remove(opId);

            if (cliFut != null)
                cliFut.onDone(msg.error());

            return false;
        }

        return onSchemaProposeDiscovery0(msg);
    }

    /**
     * Process schema propose message from discovery thread (or from cache start routine).
     *
     * @param msg Message.
     * @return {@code True} if exchange should be triggered.
     */
    private boolean onSchemaProposeDiscovery0(SchemaProposeDiscoveryMessage msg) {
        UUID opId = msg.operation().id();

        synchronized (stateMux) {
            if (disconnected) {
                if (log.isDebugEnabled())
                    log.debug("Processing discovery schema propose message, but node is disconnected (will ignore) " +
                        "[opId=" + opId + ", msg=" + msg + ']');

                return false;
            }

            if (log.isDebugEnabled())
                log.debug("Processing discovery schema propose message [opId=" + opId + ", msg=" + msg + ']');

            // Put message to active operations set.
            SchemaProposeDiscoveryMessage oldDesc = activeProposals.put(msg.operation().id(), msg);

            assert oldDesc == null;

            // Create schema operation and either trigger it immediately from exchange thread or append to already
            // running operation.
            SchemaOperation schemaOp = new SchemaOperation(msg);

            String schemaName = msg.schemaName();

            SchemaOperation prevSchemaOp = schemaOps.get(schemaName);

            if (prevSchemaOp != null) {
                prevSchemaOp = prevSchemaOp.unwind();

                if (log.isDebugEnabled())
                    log.debug("Schema change is enqueued and will be executed after previous operation is completed " +
                        "[opId=" + opId + ", prevOpId=" + prevSchemaOp.id() + ']');

                prevSchemaOp.next(schemaOp);

                return false;
            }
            else {
                schemaOps.put(schemaName, schemaOp);

                return exchangeReady;
            }
        }
    }

    /**
     * Handle schema propose from exchange thread.
     *
     * @param msg Discovery message.
     */
    @SuppressWarnings("ThrowableInstanceNeverThrown")
    public void onSchemaPropose(SchemaProposeDiscoveryMessage msg) {
        UUID opId = msg.operation().id();

        if (log.isDebugEnabled())
            log.debug("Processing schema propose message (exchange) [opId=" + opId + ']');

        synchronized (stateMux) {
            if (disconnected)
                return;

            SchemaOperation curOp = schemaOps.get(msg.schemaName());

            assert curOp != null;
            assert F.eq(opId, curOp.id());
            assert !curOp.started();

            startSchemaChange(curOp);
        }
    }

    /**
     * Process schema finish message from discovery thread.
     *
     * @param msg Message.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void onSchemaFinishDiscovery(SchemaFinishDiscoveryMessage msg) {
        UUID opId = msg.operation().id();

        if (log.isDebugEnabled())
            log.debug("Received schema finish message (discovery) [opId=" + opId + ", msg=" + msg + ']');

        synchronized (stateMux) {
            if (disconnected)
                return;

            boolean completedOpAdded = completedOpIds.add(opId);

            assert completedOpAdded;

            // Remove propose message so that it will not be shared with joining nodes.
            SchemaProposeDiscoveryMessage proposeMsg = activeProposals.remove(opId);

            assert proposeMsg != null;

            // Apply changes to public cache schema if operation is successful and original cache is still there.
            if (!msg.hasError()) {
                DynamicCacheDescriptor cacheDesc = ctx.cache().cacheDescriptor(msg.operation().cacheName());

                if (cacheDesc != null && F.eq(cacheDesc.deploymentId(), proposeMsg.deploymentId()))
                    cacheDesc.schemaChangeFinish(msg);
            }

            // Propose message will be used from exchange thread to
            msg.proposeMessage(proposeMsg);

            if (exchangeReady) {
                SchemaOperation op = schemaOps.get(proposeMsg.schemaName());

                if (F.eq(op.id(), opId)) {
                    // Completed top operation.
                    op.finishMessage(msg);

                    if (op.started())
                        op.doFinish();
                }
                else {
                    // Completed operation in the middle, will schedule completion later.
                    while (op != null) {
                        if (F.eq(op.id(), opId))
                            break;

                        op = op.next();
                    }

                    assert op != null;
                    assert !op.started();

                    op.finishMessage(msg);
                }
            }
            else {
                // Set next operation as top-level one.
                String schemaName = proposeMsg.schemaName();

                SchemaOperation op = schemaOps.remove(schemaName);

                assert op != null;
                assert F.eq(op.id(), opId);

                // Chain to the next operation (if any).
                SchemaOperation nextOp = op.next();

                if (nextOp != null)
                    schemaOps.put(schemaName, nextOp);
            }

            // Clean stale IO messages from just-joined nodes.
            cleanStaleStatusMessages(opId);
        }

        // Complete client future (if any).
        SchemaOperationClientFuture cliFut = schemaCliFuts.remove(opId);

        if (cliFut != null) {
            if (msg.hasError())
                cliFut.onDone(msg.error());
            else
                cliFut.onDone();
        }
    }

    /**
     * Initiate actual schema change operation.
     *
     * @param schemaOp Schema operation.
     */
    @SuppressWarnings({"unchecked", "ThrowableInstanceNeverThrown"})
    private void startSchemaChange(SchemaOperation schemaOp) {
        assert Thread.holdsLock(stateMux);
        assert !schemaOp.started();

        // Get current cache state.
        SchemaProposeDiscoveryMessage msg = schemaOp.proposeMessage();

        String cacheName = msg.operation().cacheName();

        DynamicCacheDescriptor cacheDesc = ctx.cache().cacheDescriptor(cacheName);

        boolean cacheExists = cacheDesc != null && F.eq(msg.deploymentId(), cacheDesc.deploymentId());

        boolean cacheRegistered = cacheExists && cacheNames.contains(cacheName);

        // Validate schema state and decide whether we should proceed or not.
        SchemaAbstractOperation op = msg.operation();

        QueryTypeDescriptorImpl type = null;
        SchemaOperationException err;

        boolean nop = false;

        if (cacheExists) {
            if (cacheRegistered) {
                // If cache is started, we perform validation against real schema.
                T3<QueryTypeDescriptorImpl, Boolean, SchemaOperationException> res = prepareChangeOnStartedCache(op);

                assert res.get2() != null;

                type = res.get1();
                nop = res.get2();
                err = res.get3();
            }
            else {
                // If cache is not started yet, there is no schema. Take schema from cache descriptor and validate.
                QuerySchema schema = cacheDesc.schema();

                T2<Boolean, SchemaOperationException> res = prepareChangeOnNotStartedCache(op, schema);

                assert res.get1() != null;

                type = null;
                nop = res.get1();
                err = res.get2();
            }
        }
        else
            err = new SchemaOperationException(SchemaOperationException.CODE_CACHE_NOT_FOUND, cacheName);

        // Start operation.
        SchemaOperationWorker worker =
            new SchemaOperationWorker(ctx, this, msg.deploymentId(), op, nop, err, cacheRegistered, type);

        SchemaOperationManager mgr = new SchemaOperationManager(ctx, this, worker,
            ctx.clientNode() ? null : coordinator());

        schemaOp.manager(mgr);

        mgr.start();

        // Unwind pending IO messages.
        if (!ctx.clientNode() && coordinator().isLocal())
            unwindPendingMessages(schemaOp.id(), mgr);

        // Schedule operation finish handling if needed.
        if (schemaOp.hasFinishMessage())
            schemaOp.doFinish();
    }

    /**
     * @return {@code true} If indexing module is in classpath and successfully initialized.
     */
    public boolean moduleEnabled() {
        return idx != null;
    }

    /**
     * @return Indexing.
     * @throws IgniteException If module is not enabled.
     */
    public GridQueryIndexing getIndexing() throws IgniteException {
        checkxEnabled();

        return idx;
    }

    /**
     * Create type descriptors from schema and initialize indexing for given cache.<p>
     * Use with {@link #busyLock} where appropriate.
     * @param cctx Cache context.
     * @param schema Initial schema.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"deprecation", "ThrowableResultOfMethodCallIgnored"})
    public void onCacheStart0(GridCacheContext<?, ?> cctx, QuerySchema schema)
        throws IgniteCheckedException {

        cctx.shared().database().checkpointReadLock();

        try {
            synchronized (stateMux) {
                boolean escape = cctx.config().isSqlEscapeAll();

                String cacheName = cctx.name();

                String schemaName = QueryUtils.normalizeSchemaName(cacheName, cctx.config().getSqlSchema());

                // Prepare candidates.
                List<Class<?>> mustDeserializeClss = new ArrayList<>();

                Collection<QueryTypeCandidate> cands = new ArrayList<>();

                Collection<QueryEntity> qryEntities = schema.entities();

                if (!F.isEmpty(qryEntities)) {
                    for (QueryEntity qryEntity : qryEntities) {
                        QueryTypeCandidate cand = QueryUtils.typeForQueryEntity(cacheName, cctx, qryEntity,
                            mustDeserializeClss, escape);

                        cands.add(cand);
                    }
                }

                // Ensure that candidates has unique index names. Otherwise we will not be able to apply pending operations.
                Map<String, QueryTypeDescriptorImpl> tblTypMap = new HashMap<>();
                Map<String, QueryTypeDescriptorImpl> idxTypMap = new HashMap<>();

                for (QueryTypeCandidate cand : cands) {
                    QueryTypeDescriptorImpl desc = cand.descriptor();

                    QueryTypeDescriptorImpl oldDesc = tblTypMap.put(desc.tableName(), desc);

                    if (oldDesc != null)
                        throw new IgniteException("Duplicate table name [cache=" + cacheName +
                            ", tblName=" + desc.tableName() + ", type1=" + desc.name() + ", type2=" + oldDesc.name() + ']');

                    for (String idxName : desc.indexes().keySet()) {
                        oldDesc = idxTypMap.put(idxName, desc);

                        if (oldDesc != null)
                            throw new IgniteException("Duplicate index name [cache=" + cacheName +
                                ", idxName=" + idxName + ", type1=" + desc.name() + ", type2=" + oldDesc.name() + ']');
                    }
                }

                // Apply pending operation which could have been completed as no-op at this point.
                // There could be only one in-flight operation for a cache.
                for (SchemaOperation op : schemaOps.values()) {
                    if (F.eq(op.proposeMessage().deploymentId(), cctx.dynamicDeploymentId())) {
                        if (op.started()) {
                            SchemaOperationWorker worker = op.manager().worker();

                            assert !worker.cacheRegistered();

                            if (!worker.nop()) {
                                IgniteInternalFuture fut = worker.future();

                                assert fut.isDone();

                                if (fut.error() == null) {
                                    SchemaAbstractOperation op0 = op.proposeMessage().operation();

                                    if (op0 instanceof SchemaIndexCreateOperation) {
                                        SchemaIndexCreateOperation opCreate = (SchemaIndexCreateOperation) op0;

                                        QueryTypeDescriptorImpl typeDesc = tblTypMap.get(opCreate.tableName());

                                        assert typeDesc != null;

                                        QueryUtils.processDynamicIndexChange(opCreate.indexName(), opCreate.index(),
                                            typeDesc);
                                    }
                                    else if (op0 instanceof SchemaIndexDropOperation) {
                                        SchemaIndexDropOperation opDrop = (SchemaIndexDropOperation) op0;

                                        QueryTypeDescriptorImpl typeDesc = idxTypMap.get(opDrop.indexName());

                                        assert typeDesc != null;

                                        QueryUtils.processDynamicIndexChange(opDrop.indexName(), null, typeDesc);
                                    }
                                    else
                                        assert false;
                                }
                            }
                        }

                        break;
                    }
                }

                // Ready to register at this point.
                registerCache0(cacheName, schemaName, cctx, cands);

                // Warn about possible implicit deserialization.
                if (!mustDeserializeClss.isEmpty()) {
                    U.warn(log, "Some classes in query configuration cannot be written in binary format " +
                        "because they either implement Externalizable interface or have writeObject/readObject " +
                        "methods. Instances of these classes will be deserialized in order to build indexes. Please " +
                        "ensure that all nodes have these classes in classpath. To enable binary serialization " +
                        "either implement " + Binarylizable.class.getSimpleName() + " interface or set explicit " +
                        "serializer using BinaryTypeConfiguration.setSerializer() method: " + mustDeserializeClss);
                }
            }
        }
        finally {
            cctx.shared().database().checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        Collection<SchemaOperationClientFuture> futs;

        synchronized (stateMux) {
            disconnected = true;
            exchangeReady = false;

            // Clear client futures.
            futs = new ArrayList<>(schemaCliFuts.values());

            schemaCliFuts.clear();

            // Clear operations data.
            activeProposals.clear();
            schemaOps.clear();
        }

        // Complete client futures outside of synchronized block because they may have listeners/chains.
        for (SchemaOperationClientFuture fut : futs)
            fut.onDone(new SchemaOperationException("Client node is disconnected (operation result is unknown)."));

        if (idx != null)
            idx.onDisconnected(reconnectFut);
    }

    /**
     * Handle cache start. Invoked either from GridCacheProcessor.onKernalStart() method or from exchange worker.
     * When called for the first time, we initialize topology thus understanding whether current node is coordinator
     * or not.
     *
     * @param cctx Cache context.
     * @param schema Index states.
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheStart(GridCacheContext cctx, QuerySchema schema) throws IgniteCheckedException {
        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            return;

        try {
            onCacheStart0(cctx, schema);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cctx Cache context.
     */
    public void onCacheStop(GridCacheContext cctx) {
        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            return;

        try {
            onCacheStop0(cctx.name());
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @return Skip field lookup flag.
     */
    public boolean skipFieldLookup() {
        return skipFieldLookup;
    }

    /**
     * @param skipFieldLookup Skip field lookup flag.
     */
    public void skipFieldLookup(boolean skipFieldLookup) {
        this.skipFieldLookup = skipFieldLookup;
    }

    /**
     * Handle custom discovery message.
     *
     * @param msg Message.
     */
    public void onDiscovery(SchemaAbstractDiscoveryMessage msg) {
        IgniteUuid id = msg.id();

        if (!dscoMsgIdHist.add(id)) {
            U.warn(log, "Received duplicate schema custom discovery message (will ignore) [opId=" +
                msg.operation().id() + ", msg=" + msg  +']');

            return;
        }

        if (msg instanceof SchemaProposeDiscoveryMessage) {
            SchemaProposeDiscoveryMessage msg0 = (SchemaProposeDiscoveryMessage)msg;

            boolean exchange = onSchemaProposeDiscovery(msg0);

            msg0.exchange(exchange);
        }
        else if (msg instanceof SchemaFinishDiscoveryMessage) {
            SchemaFinishDiscoveryMessage msg0 = (SchemaFinishDiscoveryMessage)msg;

            onSchemaFinishDiscovery(msg0);
        }
        else
            U.warn(log, "Received unsupported schema custom discovery message (will ignore) [opId=" +
                msg.operation().id() + ", msg=" + msg  +']');
    }

    /**
     * Prepare change on started cache.
     *
     * @param op Operation.
     * @return Result: affected type, nop flag, error.
     */
    private T3<QueryTypeDescriptorImpl, Boolean, SchemaOperationException> prepareChangeOnStartedCache(
        SchemaAbstractOperation op) {
        QueryTypeDescriptorImpl type = null;
        boolean nop = false;
        SchemaOperationException err = null;

        String cacheName = op.cacheName();

        if (op instanceof SchemaIndexCreateOperation) {
            SchemaIndexCreateOperation op0 = (SchemaIndexCreateOperation) op;

            QueryIndex idx = op0.index();

            // Make sure table exists.
            String tblName = op0.tableName();

            type = type(cacheName, tblName);

            if (type == null)
                err = new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, tblName);
            else {
                // Make sure that index can be applied to the given table.
                for (String idxField : idx.getFieldNames()) {
                    if (!type.fields().containsKey(idxField)) {
                        err = new SchemaOperationException(SchemaOperationException.CODE_COLUMN_NOT_FOUND,
                            idxField);

                        break;
                    }
                }
            }

            // Check conflict with other indexes.
            if (err == null) {
                String idxName = op0.index().getName();

                QueryIndexKey idxKey = new QueryIndexKey(op.schemaName(), idxName);

                if (idxs.get(idxKey) != null) {
                    if (op0.ifNotExists())
                        nop = true;
                    else
                        err = new SchemaOperationException(SchemaOperationException.CODE_INDEX_EXISTS, idxName);
                }
            }
        }
        else if (op instanceof SchemaIndexDropOperation) {
            SchemaIndexDropOperation op0 = (SchemaIndexDropOperation) op;

            String idxName = op0.indexName();

            QueryIndexDescriptorImpl oldIdx = idxs.get(new QueryIndexKey(op.schemaName(), idxName));

            if (oldIdx == null) {
                if (op0.ifExists())
                    nop = true;
                else
                    err = new SchemaOperationException(SchemaOperationException.CODE_INDEX_NOT_FOUND, idxName);
            }
            else
                type = oldIdx.typeDescriptor();
        }
        else
            err = new SchemaOperationException("Unsupported operation: " + op);

        return new T3<>(type, nop, err);
    }

    /**
     * Prepare operation on non-started cache.
     *
     * @param op Operation.
     * @param schema Known cache schema.
     * @return Result: nop flag, error.
     */
    private T2<Boolean, SchemaOperationException> prepareChangeOnNotStartedCache(SchemaAbstractOperation op,
        QuerySchema schema) {
        boolean nop = false;
        SchemaOperationException err = null;

        // Build table and index maps.
        Map<String, QueryEntity> tblMap = new HashMap<>();
        Map<String, T2<QueryEntity, QueryIndex>> idxMap = new HashMap<>();

        for (QueryEntity entity : schema.entities()) {
            String tblName = entity.getTableName();

            QueryEntity oldEntity = tblMap.put(tblName, entity);

            if (oldEntity != null) {
                err = new SchemaOperationException("Invalid schema state (duplicate table found): " + tblName);

                break;
            }

            for (QueryIndex entityIdx : entity.getIndexes()) {
                String idxName = entityIdx.getName();

                T2<QueryEntity, QueryIndex> oldIdxEntity = idxMap.put(idxName, new T2<>(entity, entityIdx));

                if (oldIdxEntity != null) {
                    err = new SchemaOperationException("Invalid schema state (duplicate index found): " +
                        idxName);

                    break;
                }
            }

            if (err != null)
                break;
        }

        // Now check whether operation can be applied to schema.
        if (op instanceof SchemaIndexCreateOperation) {
            SchemaIndexCreateOperation op0 = (SchemaIndexCreateOperation)op;

            String idxName = op0.indexName();

            T2<QueryEntity, QueryIndex> oldIdxEntity = idxMap.get(idxName);

            if (oldIdxEntity == null) {
                String tblName = op0.tableName();

                QueryEntity oldEntity = tblMap.get(tblName);

                if (oldEntity == null)
                    err = new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, tblName);
                else {
                    for (String fieldName : op0.index().getFields().keySet()) {
                        Set<String> oldEntityFields = new HashSet<>(oldEntity.getFields().keySet());

                        for (Map.Entry<String, String> alias : oldEntity.getAliases().entrySet()) {
                            oldEntityFields.remove(alias.getKey());
                            oldEntityFields.add(alias.getValue());
                        }

                        if (!oldEntityFields.contains(fieldName)) {
                            err = new SchemaOperationException(SchemaOperationException.CODE_COLUMN_NOT_FOUND,
                                fieldName);

                            break;
                        }
                    }
                }
            }
            else {
                if (op0.ifNotExists())
                    nop = true;
                else
                    err = new SchemaOperationException(SchemaOperationException.CODE_INDEX_EXISTS, idxName);
            }
        }
        else if (op instanceof SchemaIndexDropOperation) {
            SchemaIndexDropOperation op0 = (SchemaIndexDropOperation)op;

            String idxName = op0.indexName();

            T2<QueryEntity, QueryIndex> oldIdxEntity = idxMap.get(idxName);

            if (oldIdxEntity == null) {
                if (op0.ifExists())
                    nop = true;
                else
                    err = new SchemaOperationException(SchemaOperationException.CODE_INDEX_NOT_FOUND, idxName);
            }
        }
        else
            err = new SchemaOperationException("Unsupported operation: " + op);

        return new T2<>(nop, err);
    }

    /**
     * Invoked when coordinator finished ensuring that all participants are ready.
     *
     * @param op Operation.
     * @param err Error (if any).
     */
    public void onCoordinatorFinished(SchemaAbstractOperation op, @Nullable SchemaOperationException err) {
        synchronized (stateMux) {
            SchemaFinishDiscoveryMessage msg = new SchemaFinishDiscoveryMessage(op, err);

            try {
                ctx.discovery().sendCustomEvent(msg);
            }
            catch (Exception e) {
                // Failed to send finish message over discovery. This is something unrecoverable.
                U.warn(log, "Failed to send schema finish discovery message [opId=" + op.id() + ']', e);
            }
        }
    }

    /**
     * Get current coordinator node.
     *
     * @return Coordinator node.
     */
    private ClusterNode coordinator() {
        assert !ctx.clientNode();

        synchronized (stateMux) {
            if (crd == null) {
                ClusterNode crd0 = null;

                for (ClusterNode node : ctx.discovery().aliveServerNodes()) {
                    if (crd0 == null || crd0.order() > node.order())
                        crd0 = node;
                }

                assert crd0 != null;

                crd = crd0;
            }

            return crd;
        }
    }

    /**
     * Get rid of stale IO message received from other nodes which joined when operation had been in progress.
     *
     * @param opId Operation ID.
     */
    private void cleanStaleStatusMessages(UUID opId) {
        Iterator<SchemaOperationStatusMessage> it = pendingMsgs.iterator();

        while (it.hasNext()) {
            SchemaOperationStatusMessage statusMsg = it.next();

            if (F.eq(opId, statusMsg.operationId())) {
                it.remove();

                if (log.isDebugEnabled())
                    log.debug("Dropped operation status message because it is already completed [opId=" + opId +
                        ", rmtNode=" + statusMsg.senderNodeId() + ']');
            }
        }
    }

    /**
     * Apply positive index operation result.
     *
     * @param op Operation.
     * @param type Type descriptor (if available),
     */
    public void onLocalOperationFinished(SchemaAbstractOperation op, @Nullable QueryTypeDescriptorImpl type) {
        synchronized (stateMux) {
            if (disconnected)
                return;

            // No need to apply anything to obsolete type.
            if (type == null || type.obsolete()) {
                if (log.isDebugEnabled())
                    log.debug("Local operation finished, but type descriptor is either missing or obsolete " +
                        "(will ignore) [opId=" + op.id() + ']');

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Local operation finished successfully [opId=" + op.id() + ']');

            String schemaName = op.schemaName();

            try {
                if (op instanceof SchemaIndexCreateOperation) {
                    SchemaIndexCreateOperation op0 = (SchemaIndexCreateOperation)op;

                    QueryUtils.processDynamicIndexChange(op0.indexName(), op0.index(), type);

                    QueryIndexDescriptorImpl idxDesc = type.index(op0.indexName());

                    QueryIndexKey idxKey = new QueryIndexKey(schemaName, op0.indexName());

                    idxs.put(idxKey, idxDesc);
                }
                else {
                    assert op instanceof SchemaIndexDropOperation;

                    SchemaIndexDropOperation op0 = (SchemaIndexDropOperation) op;

                    QueryUtils.processDynamicIndexChange(op0.indexName(), null, type);

                    QueryIndexKey idxKey = new QueryIndexKey(schemaName, op0.indexName());

                    idxs.remove(idxKey);
                }
            }
            catch (IgniteCheckedException e) {
                U.warn(log, "Failed to finish index operation [opId=" + op.id() + " op=" + op + ']', e);
            }
        }
    }

    /**
     * Handle node leave.
     *
     * @param node Node.
     */
    public void onNodeLeave(ClusterNode node) {
        synchronized (stateMux) {
            // Clients do not send status messages and are never coordinators.
            if (ctx.clientNode())
                return;

            ClusterNode crd0 = coordinator();

            if (F.eq(node.id(), crd0.id())) {
                crd = null;

                crd0 = coordinator();
            }

            for (SchemaOperation op : schemaOps.values()) {
                if (op.started()) {
                    op.manager().onNodeLeave(node.id(), crd0);

                    if (crd0.isLocal())
                        unwindPendingMessages(op.id(), op.manager());
                }
            }
        }
    }

    /**
     * Process index operation.
     *
     * @param op Operation.
     * @param type Type descriptor.
     * @param depId Cache deployment ID.
     * @param cancelTok Cancel token.
     * @throws SchemaOperationException If failed.
     */
    public void processIndexOperationLocal(SchemaAbstractOperation op, QueryTypeDescriptorImpl type, IgniteUuid depId,
        SchemaIndexOperationCancellationToken cancelTok) throws SchemaOperationException {
        if (log.isDebugEnabled())
            log.debug("Started local index operation [opId=" + op.id() + ']');

        String cacheName = op.cacheName();

        GridCacheAdapter cache = ctx.cache().internalCache(cacheName);

        if (cache == null || !F.eq(depId, cache.context().dynamicDeploymentId()))
            throw new SchemaOperationException(SchemaOperationException.CODE_CACHE_NOT_FOUND, cacheName);

        try {
            if (op instanceof SchemaIndexCreateOperation) {
                SchemaIndexCreateOperation op0 = (SchemaIndexCreateOperation) op;

                QueryIndexDescriptorImpl idxDesc = QueryUtils.createIndexDescriptor(type, op0.index());

                SchemaIndexCacheVisitor visitor =
                    new SchemaIndexCacheVisitorImpl(this, cache.context(), cacheName, op0.tableName(), cancelTok);

                idx.dynamicIndexCreate(op0.schemaName(), op0.tableName(), idxDesc, op0.ifNotExists(), visitor);
            }
            else if (op instanceof SchemaIndexDropOperation) {
                SchemaIndexDropOperation op0 = (SchemaIndexDropOperation) op;

                idx.dynamicIndexDrop(op0.schemaName(), op0.indexName(), op0.ifExists());
            }
            else
                throw new SchemaOperationException("Unsupported operation: " + op);
        }
        catch (Exception e) {
            if (e instanceof SchemaOperationException)
                throw (SchemaOperationException)e;
            else
                throw new SchemaOperationException("Schema change operation failed: " + e.getMessage(), e);
        }
    }

    /**
     * Create cache and table from given query entity.
     *
     * @param schemaName Schema name to create table in.
     * @param entity Entity to create table from.
     * @param templateName Template name.
     * @param cacheGroup Cache group name.
     * @param affinityKey Affinity key column name.
     * @param atomicityMode Atomicity mode.
     * @param backups Backups.
     * @param ifNotExists Quietly ignore this command if table already exists.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public void dynamicTableCreate(String schemaName, QueryEntity entity, String templateName, String cacheGroup,
        String affinityKey, @Nullable CacheAtomicityMode atomicityMode, int backups, boolean ifNotExists)
        throws IgniteCheckedException {
        assert !F.isEmpty(templateName);
        assert backups >= 0;

        CacheConfiguration<?, ?> ccfg = ctx.cache().getConfigFromTemplate(templateName);

        if (ccfg == null) {
            if (QueryUtils.TEMPLATE_PARTITIONED.equalsIgnoreCase(templateName))
                ccfg = new CacheConfiguration<>().setCacheMode(CacheMode.PARTITIONED);
            else if (QueryUtils.TEMPLATE_REPLICÄTED.equalsIgnoreCase(templateName))
                ccfg = new CacheConfiguration<>().setCacheMode(CacheMode.REPLICATED);
            else
                throw new SchemaOperationException(SchemaOperationException.CODE_CACHE_NOT_FOUND, templateName);
        }

        if (!F.isEmpty(ccfg.getQueryEntities()))
            throw new SchemaOperationException("Template cache already contains query entities which it should not: " +
                templateName);

        ccfg.setName(QueryUtils.createTableCacheName(schemaName, entity.getTableName()));

        if (!F.isEmpty(cacheGroup))
            ccfg.setGroupName(cacheGroup);

        if (atomicityMode != null)
            ccfg.setAtomicityMode(atomicityMode);

        ccfg.setBackups(backups);

        ccfg.setSqlSchema(schemaName);
        ccfg.setSqlEscapeAll(true);
        ccfg.setQueryEntities(Collections.singleton(entity));

        if (affinityKey != null) {
            assert entity.getFields().containsKey(affinityKey) && entity.getKeyFields().contains(affinityKey);

            ccfg.setAffinityMapper(new DynamicTableAffinityKeyMapper(entity.getKeyType(), affinityKey));
        }

        boolean res;

        try {
            res = ctx.grid().getOrCreateCache0(ccfg, true).get2();
        }
        catch (CacheException e) {
            if (e.getCause() instanceof SchemaOperationException)
                throw (SchemaOperationException)e.getCause();
            else
                throw e;
        }

        if (!res && !ifNotExists)
            throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_EXISTS,  entity.getTableName());
    }

    /**
     * Drop table by destroying its cache if it's an 1:1 per cache table.
     *
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param ifExists Quietly ignore this command if table does not exist.
     * @throws SchemaOperationException if {@code ifExists} is {@code false} and cache was not found.
     */
    @SuppressWarnings("unchecked")
    public void dynamicTableDrop(String cacheName, String tblName, boolean ifExists) throws SchemaOperationException {
        boolean res = ctx.grid().destroyCache0(cacheName, true);

        if (!res && !ifExists)
            throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, tblName);
    }

    /**
     * Register cache in indexing SPI.
     *
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param cctx Cache context.
     * @param cands Candidates.
     * @throws IgniteCheckedException If failed.
     */
    private void registerCache0(String cacheName, String schemaName, GridCacheContext<?, ?> cctx,
        Collection<QueryTypeCandidate> cands) throws IgniteCheckedException {
        synchronized (stateMux) {
            if (idx != null)
                idx.registerCache(cacheName, schemaName, cctx);

            try {
                for (QueryTypeCandidate cand : cands) {
                    QueryTypeIdKey typeId = cand.typeId();
                    QueryTypeIdKey altTypeId = cand.alternativeTypeId();
                    QueryTypeDescriptorImpl desc = cand.descriptor();

                    if (typesByName.putIfAbsent(new QueryTypeNameKey(cacheName, desc.name()), desc) != null)
                        throw new IgniteCheckedException("Type with name '" + desc.name() + "' already indexed " +
                            "in cache '" + cacheName + "'.");

                    types.put(typeId, desc);

                    if (altTypeId != null)
                        types.put(altTypeId, desc);

                    for (QueryIndexDescriptorImpl idx : desc.indexes0()) {
                        QueryIndexKey idxKey = new QueryIndexKey(schemaName, idx.name());

                        QueryIndexDescriptorImpl oldIdx = idxs.putIfAbsent(idxKey, idx);

                        if (oldIdx != null) {
                            throw new IgniteException("Duplicate index name [cache=" + cacheName +
                                ", schemaName=" + schemaName + ", idxName=" + idx.name() +
                                ", existingTable=" + oldIdx.typeDescriptor().tableName() +
                                ", table=" + desc.tableName() + ']');
                        }
                    }

                    if (idx != null)
                        idx.registerType(cctx, desc);
                }

                cacheNames.add(CU.mask(cacheName));
            }
            catch (IgniteCheckedException | RuntimeException e) {
                onCacheStop0(cacheName);

                throw e;
            }
        }
    }

    /**
     * Unregister cache.<p>
     * Use with {@link #busyLock} where appropriate.
     *
     * @param cacheName Cache name.
     */
    public void onCacheStop0(String cacheName) {
        if (idx == null)
            return;

        synchronized (stateMux) {
            // Clear types.
            Iterator<Map.Entry<QueryTypeIdKey, QueryTypeDescriptorImpl>> it = types.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<QueryTypeIdKey, QueryTypeDescriptorImpl> entry = it.next();

                if (F.eq(cacheName, entry.getKey().cacheName())) {
                    it.remove();

                    typesByName.remove(new QueryTypeNameKey(cacheName, entry.getValue().name()));

                    entry.getValue().markObsolete();
                }
            }

            // Clear indexes.
            Iterator<Map.Entry<QueryIndexKey, QueryIndexDescriptorImpl>> idxIt = idxs.entrySet().iterator();

            while (idxIt.hasNext()) {
                Map.Entry<QueryIndexKey, QueryIndexDescriptorImpl> idxEntry = idxIt.next();

                if (F.eq(cacheName, idxEntry.getValue().typeDescriptor().cacheName()))
                    idxIt.remove();
            }

            // Notify in-progress index operations.
            for (SchemaOperation op : schemaOps.values()) {
                if (op.started())
                    op.manager().worker().cancel();
            }

            // Notify indexing.
            try {
                idx.unregisterCache(cacheName);
            }
            catch (Exception e) {
                U.error(log, "Failed to clear indexing on cache unregister (will ignore): " + cacheName, e);
            }

            cacheNames.remove(cacheName);
        }
    }

    /**
     * Check whether provided key and value belongs to expected cache and table.
     *
     * @param cctx Target cache context.
     * @param expCacheName Expected cache name.
     * @param expTblName Expected table name.
     * @param key Key.
     * @param val Value.
     * @return {@code True} if this key-value pair belongs to expected cache/table, {@code false} otherwise or
     *     if cache or table doesn't exist.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public boolean belongsToTable(GridCacheContext cctx, String expCacheName, String expTblName, KeyCacheObject key,
        CacheObject val) throws IgniteCheckedException {
        QueryTypeDescriptorImpl desc = type(expCacheName, val);

        if (desc == null)
            return false;

        if (!F.eq(expTblName, desc.tableName()))
            return false;

        if (!cctx.cacheObjects().isBinaryObject(val)) {
            Class<?> valCls = val.value(cctx.cacheObjectContext(), false).getClass();

            if (!desc.valueClass().isAssignableFrom(valCls))
                return false;
        }

        if (!cctx.cacheObjects().isBinaryObject(key)) {
            Class<?> keyCls = key.value(cctx.cacheObjectContext(), false).getClass();

            if (!desc.keyClass().isAssignableFrom(keyCls))
                return false;
        }

        return true;
    }

    /**
     * Rebuilds indexes for provided caches from corresponding hash indexes.
     *
     * @param cacheIds Cache IDs.
     * @return Future that will be completed when rebuilding is finished.
     */
    public IgniteInternalFuture<?> rebuildIndexesFromHash(Collection<Integer> cacheIds) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to rebuild indexes from hash (grid is stopping).");

        try {
            GridCompoundFuture<Object, ?> fut = new GridCompoundFuture<Object, Object>();

            for (Map.Entry<QueryTypeIdKey, QueryTypeDescriptorImpl> e : types.entrySet()) {
                if (cacheIds.contains(CU.cacheId(e.getKey().cacheName())))
                    fut.add(rebuildIndexesFromHash(e.getKey().cacheName(), e.getValue()));
            }

            fut.markInitialized();

            return fut;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cacheName Cache name.
     * @param desc Type descriptor.
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    private IgniteInternalFuture<Object> rebuildIndexesFromHash(@Nullable final String cacheName,
        @Nullable final QueryTypeDescriptorImpl desc) {
        if (idx == null)
            return new GridFinishedFuture<>(new IgniteCheckedException("Indexing is disabled."));

        if (desc == null)
            return new GridFinishedFuture<>();

        final GridWorkerFuture<Object> fut = new GridWorkerFuture<>();

        final String schemaName = idx.schema(cacheName);
        final String typeName = desc.name();

        idx.markForRebuildFromHash(schemaName, typeName);

        GridWorker w = new GridWorker(ctx.igniteInstanceName(), "index-rebuild-worker", log) {
            @Override protected void body() {
                try {
                    int cacheId = CU.cacheId(cacheName);

                    GridCacheContext cctx = ctx.cache().context().cacheContext(cacheId);

                    idx.rebuildIndexesFromHash(cctx, schemaName, typeName);

                    fut.onDone();
                }
                catch (Exception e) {
                    fut.onDone(e);
                }
                catch (Throwable e) {
                    U.error(log, "Failed to rebuild indexes for type [cache=" + cacheName +
                        ", type=" + typeName + ']', e);

                    fut.onDone(e);

                    throw e;
                }
            }
        };

        fut.setWorker(w);

        ctx.getExecutorService().execute(w);

        return fut;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache object context.
     */
    private CacheObjectContext cacheObjectContext(String cacheName) {
        return ctx.cache().internalCache(cacheName).context().cacheObjectContext();
    }

    /**
     * Writes key-value pair to index.
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @param ver Cache entry version.
     * @param expirationTime Expiration time or 0 if never expires.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    public void store(final String cacheName,
        final KeyCacheObject key,
        int partId,
        @Nullable CacheObject prevVal,
        @Nullable GridCacheVersion prevVer,
        final CacheObject val,
        GridCacheVersion ver,
        long expirationTime,
        long link) throws IgniteCheckedException {
        assert key != null;
        assert val != null;

        if (log.isDebugEnabled())
            log.debug("Store [cache=" + cacheName + ", key=" + key + ", val=" + val + "]");

        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

        try {
            CacheObjectContext coctx = cacheObjectContext(cacheName);

            QueryTypeDescriptorImpl desc = typeByValue(cacheName, coctx, key, val, true);

            if (prevVal != null) {
                QueryTypeDescriptorImpl prevValDesc = typeByValue(cacheName, coctx, key, prevVal, false);

                if (prevValDesc != null && prevValDesc != desc)
                    idx.remove(cacheName, prevValDesc, key, partId, prevVal, prevVer);
            }

            if (desc == null)
                return;

            idx.store(cacheName, desc, key, partId, val, ver, expirationTime, link);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cacheName Cache name.
     * @param coctx Cache context.
     * @param key Key.
     * @param val Value.
     * @param checkType If {@code true} checks that key and value type correspond to found TypeDescriptor.
     * @return Type descriptor if found.
     * @throws IgniteCheckedException If type check failed.
     */
    @SuppressWarnings("ConstantConditions")
    @Nullable private QueryTypeDescriptorImpl typeByValue(String cacheName,
        CacheObjectContext coctx,
        KeyCacheObject key,
        CacheObject val,
        boolean checkType)
        throws IgniteCheckedException {
        Class<?> valCls = null;

        QueryTypeIdKey id;

        boolean binaryVal = ctx.cacheObjects().isBinaryObject(val);

        if (binaryVal) {
            int typeId = ctx.cacheObjects().typeId(val);

            id = new QueryTypeIdKey(cacheName, typeId);
        }
        else {
            valCls = val.value(coctx, false).getClass();

            id = new QueryTypeIdKey(cacheName, valCls);
        }

        QueryTypeDescriptorImpl desc = types.get(id);

        if (desc == null)
            return null;

        if (checkType) {
            if (!binaryVal && !desc.valueClass().isAssignableFrom(valCls))
                throw new IgniteCheckedException("Failed to update index due to class name conflict" +
                    "(multiple classes with same simple name are stored in the same cache) " +
                    "[expCls=" + desc.valueClass().getName() + ", actualCls=" + valCls.getName() + ']');

            if (!ctx.cacheObjects().isBinaryObject(key)) {
                Class<?> keyCls = key.value(coctx, false).getClass();

                if (!desc.keyClass().isAssignableFrom(keyCls))
                    throw new IgniteCheckedException("Failed to update index, incorrect key class [expCls=" +
                        desc.keyClass().getName() + ", actualCls=" + keyCls.getName() + "]");
            }
        }

        return desc;
    }

    /**
     * Gets type descriptor for cache by given object's type.
     *
     * @param cacheName Cache name.
     * @param val Object to determine type for.
     * @return Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private QueryTypeDescriptorImpl type(@Nullable String cacheName, CacheObject val) throws IgniteCheckedException {
        CacheObjectContext coctx = cacheObjectContext(cacheName);

        QueryTypeIdKey id;

        boolean binaryVal = ctx.cacheObjects().isBinaryObject(val);

        if (binaryVal)
            id = new QueryTypeIdKey(cacheName, ctx.cacheObjects().typeId(val));
        else
            id = new QueryTypeIdKey(cacheName, val.value(coctx, false).getClass());

        return types.get(id);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void checkEnabled() throws IgniteCheckedException {
        if (idx == null)
            throw new IgniteCheckedException("Indexing is disabled.");
    }

    /**
     * @throws IgniteException If indexing is disabled.
     */
    private void checkxEnabled() throws IgniteException {
        if (idx == null)
            throw new IgniteException("Failed to execute query because indexing is disabled (consider adding module " +
                INDEXING.module() + " to classpath or moving it from 'optional' to 'libs' folder).");
    }

    /**
     * Query SQL fields.
     *
     * @param cctx Cache context.
     * @param qry Query.
     * @param keepBinary Keep binary flag.
     * @return Cursor.
     */
    @SuppressWarnings("unchecked")
    public FieldsQueryCursor<List<?>> querySqlFields(final GridCacheContext<?,?> cctx, final SqlFieldsQuery qry,
        final boolean keepBinary) {
        checkxEnabled();

        validateSqlFieldsQuery(qry);

        boolean loc = (qry.isReplicatedOnly() && cctx.isReplicatedAffinityNode()) || cctx.isLocal() || qry.isLocal();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            final String schemaName = qry.getSchema() != null ? qry.getSchema() : idx.schema(cctx.name());
            final int mainCacheId = CU.cacheId(cctx.name());

            IgniteOutClosureX<FieldsQueryCursor<List<?>>> clo;

            if (loc) {
                clo = new IgniteOutClosureX<FieldsQueryCursor<List<?>>>() {
                    @Override public FieldsQueryCursor<List<?>> applyx() throws IgniteCheckedException {
                        GridQueryCancel cancel = new GridQueryCancel();

                        FieldsQueryCursor<List<?>> cur;

                        if (cctx.config().getQueryParallelism() > 1) {
                            qry.setDistributedJoins(true);

                            cur = idx.queryDistributedSqlFields(schemaName, qry, keepBinary, cancel, mainCacheId);
                        }
                        else {
                            IndexingQueryFilter filter = idx.backupFilter(requestTopVer.get(), qry.getPartitions());

                            cur = idx.queryLocalSqlFields(schemaName, qry, keepBinary, filter, cancel);
                        }

                        sendQueryExecutedEvent(qry.getSql(), qry.getArgs(), cctx.name());

                        return cur;
                    }
                };
            }
            else {
                clo = new IgniteOutClosureX<FieldsQueryCursor<List<?>>>() {
                    @Override public FieldsQueryCursor<List<?>> applyx() throws IgniteCheckedException {
                        return idx.queryDistributedSqlFields(schemaName, qry, keepBinary, null, mainCacheId);
                    }
                };
            }

            return executeQuery(GridCacheQueryType.SQL_FIELDS, qry.getSql(), cctx, clo, true);
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Query SQL fields without strict dependency on concrete cache.
     *
     * @param qry Query.
     * @param keepBinary Keep binary flag.
     * @return Cursor.
     */
    public FieldsQueryCursor<List<?>> querySqlFieldsNoCache(final SqlFieldsQuery qry, final boolean keepBinary) {
        checkxEnabled();

        validateSqlFieldsQuery(qry);

        if (qry.isLocal())
            throw new IgniteException("Local query is not supported without specific cache.");

        if (qry.getSchema() == null)
            qry.setSchema(QueryUtils.DFLT_SCHEMA);

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            IgniteOutClosureX<FieldsQueryCursor<List<?>>> clo = new IgniteOutClosureX<FieldsQueryCursor<List<?>>>() {
                @Override public FieldsQueryCursor<List<?>> applyx() throws IgniteCheckedException {
                    GridQueryCancel cancel = new GridQueryCancel();

                    return idx.queryDistributedSqlFields(qry.getSchema(), qry, keepBinary, cancel, null);
                }
            };

            return executeQuery(GridCacheQueryType.SQL_FIELDS, qry.getSql(), null, clo, true);
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Validate SQL fields query.
     *
     * @param qry Query.
     */
    private static void validateSqlFieldsQuery(SqlFieldsQuery qry) {
        if (qry.isReplicatedOnly() && qry.getPartitions() != null)
            throw new CacheException("Partitions are not supported in replicated only mode.");

        if (qry.isDistributedJoins() && qry.getPartitions() != null)
            throw new CacheException("Using both partitions and distributed JOINs is not supported for the same query");
    }

    /**
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param streamer Data streamer.
     * @param qry Query.
     * @return Iterator.
     */
    public long streamUpdateQuery(@Nullable final String cacheName, final String schemaName,
        final IgniteDataStreamer<?, ?> streamer, final String qry, final Object[] args) {
        assert streamer != null;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            GridCacheContext cctx = ctx.cache().cache(cacheName).context();

            return executeQuery(GridCacheQueryType.SQL_FIELDS, qry, cctx, new IgniteOutClosureX<Long>() {
                @Override public Long applyx() throws IgniteCheckedException {
                    return idx.streamUpdateQuery(schemaName, qry, args, streamer);
                }
            }, true);
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Execute distributed SQL query.
     *
     * @param cctx Cache context.
     * @param qry Query.
     * @param keepBinary Keep binary flag.
     * @return Cursor.
     */
    public <K, V> QueryCursor<Cache.Entry<K,V>> querySql(final GridCacheContext<?,?> cctx, final SqlQuery qry,
        boolean keepBinary) {
        if (qry.isReplicatedOnly() && qry.getPartitions() != null)
            throw new CacheException("Partitions are not supported in replicated only mode.");

        if (qry.isDistributedJoins() && qry.getPartitions() != null)
            throw new CacheException(
                "Using both partitions and distributed JOINs is not supported for the same query");

        if ((qry.isReplicatedOnly() && cctx.isReplicatedAffinityNode()) || cctx.isLocal() || qry.isLocal())
            return queryLocalSql(cctx, qry, keepBinary);

        return queryDistributedSql(cctx, qry, keepBinary);
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @param keepBinary Keep binary flag.
     * @return Cursor.
     */
    private <K,V> QueryCursor<Cache.Entry<K,V>> queryDistributedSql(final GridCacheContext<?,?> cctx,
        final SqlQuery qry, final boolean keepBinary) {
        checkxEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            final String schemaName = idx.schema(cctx.name());
            final int mainCacheId = CU.cacheId(cctx.name());

            return executeQuery(GridCacheQueryType.SQL, qry.getSql(), cctx,
                new IgniteOutClosureX<QueryCursor<Cache.Entry<K, V>>>() {
                    @Override public QueryCursor<Cache.Entry<K, V>> applyx() throws IgniteCheckedException {
                        return idx.queryDistributedSql(schemaName, qry, keepBinary, mainCacheId);
                    }
                }, true);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @param keepBinary Keep binary flag.
     * @return Cursor.
     */
    private <K, V> QueryCursor<Cache.Entry<K, V>> queryLocalSql(final GridCacheContext<?, ?> cctx, final SqlQuery qry,
        final boolean keepBinary) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        final String schemaName = idx.schema(cctx.name());
        final int mainCacheId = CU.cacheId(cctx.name());

        try {
            return executeQuery(GridCacheQueryType.SQL, qry.getSql(), cctx,
                new IgniteOutClosureX<QueryCursor<Cache.Entry<K, V>>>() {
                    @Override public QueryCursor<Cache.Entry<K, V>> applyx() throws IgniteCheckedException {
                        String type = qry.getType();

                        String typeName = typeName(cctx.name(), type);

                        qry.setType(typeName);

                        sendQueryExecutedEvent(
                            qry.getSql(),
                            qry.getArgs(),
                            cctx.name());

                        if (cctx.config().getQueryParallelism() > 1) {
                            qry.setDistributedJoins(true);

                            return idx.queryDistributedSql(schemaName, qry, keepBinary, mainCacheId);
                        }
                        else
                            return idx.queryLocalSql(schemaName, qry, idx.backupFilter(requestTopVer.get(),
                                qry.getPartitions()), keepBinary);
                    }
                }, true);
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Collect queries that already running more than specified duration.
     *
     * @param duration Duration to check.
     * @return Collection of long running queries.
     */
    public Collection<GridRunningQueryInfo> runningQueries(long duration) {
        if (moduleEnabled())
            return idx.runningQueries(duration);

        return Collections.emptyList();
    }

    /**
     * Cancel specified queries.
     *
     * @param queries Queries ID's to cancel.
     */
    public void cancelQueries(Collection<Long> queries) {
        if (moduleEnabled())
            idx.cancelQueries(queries);
    }

    /**
     * Entry point for index procedure.
     *
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idx Index.
     * @param ifNotExists When set to {@code true} operation will fail if index already exists.
     * @return Future completed when index is created.
     */
    public IgniteInternalFuture<?> dynamicIndexCreate(String cacheName, String schemaName, String tblName,
        QueryIndex idx, boolean ifNotExists) {
        SchemaAbstractOperation op = new SchemaIndexCreateOperation(UUID.randomUUID(), cacheName, schemaName, tblName,
            idx, ifNotExists);

        return startIndexOperationDistributed(op);
    }

    /**
     * Entry point for index drop procedure
     *
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param idxName Index name.
     * @param ifExists When set to {@code true} operation fill fail if index doesn't exists.
     * @return Future completed when index is created.
     */
    public IgniteInternalFuture<?> dynamicIndexDrop(String cacheName, String schemaName, String idxName,
        boolean ifExists) {
        SchemaAbstractOperation op = new SchemaIndexDropOperation(UUID.randomUUID(), cacheName, schemaName, idxName,
            ifExists);

        return startIndexOperationDistributed(op);
    }

    /**
     * Start distributed index change operation.
     *
     * @param op Operation.
     * @return Future.
     */
    private IgniteInternalFuture<?> startIndexOperationDistributed(SchemaAbstractOperation op) {
        SchemaOperationClientFuture fut = new SchemaOperationClientFuture(op.id());

        SchemaOperationClientFuture oldFut = schemaCliFuts.put(op.id(), fut);

        assert oldFut == null;

        try {
            ctx.discovery().sendCustomEvent(new SchemaProposeDiscoveryMessage(op));

            if (log.isDebugEnabled())
                log.debug("Sent schema propose discovery message [opId=" + op.id() + ", op=" + op + ']');

            boolean disconnected0;

            synchronized (stateMux) {
                disconnected0 = disconnected;
            }

            if (disconnected0) {
                fut.onDone(new SchemaOperationException("Client node is disconnected (operation result is unknown)."));

                schemaCliFuts.remove(op.id());
            }
        }
        catch (Exception e) {
            if (e instanceof SchemaOperationException)
                fut.onDone(e);
            else {
                fut.onDone(new SchemaOperationException("Failed to start schema change operation due to " +
                    "unexpected exception [opId=" + op.id() + ", op=" + op + ']', e));
            }

            schemaCliFuts.remove(op.id());
        }

        return fut;
    }

    /**
     * @param sqlQry Sql query.
     * @param params Params.
     */
    private void sendQueryExecutedEvent(String sqlQry, Object[] params, String cacheName) {
        if (ctx.event().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
            ctx.event().record(new CacheQueryExecutedEvent<>(
                ctx.discovery().localNode(),
                "SQL query executed.",
                EVT_CACHE_QUERY_EXECUTED,
                CacheQueryType.SQL.name(),
                cacheName,
                null,
                sqlQry,
                null,
                null,
                params,
                null,
                null));
        }
    }

    /**
     *
     * @param cacheName Cache name.
     * @param sql Query.
     * @return {@link PreparedStatement} from underlying engine to supply metadata to Prepared - most likely H2.
     */
    public PreparedStatement prepareNativeStatement(String cacheName, String sql) throws SQLException {
        checkxEnabled();

        String schemaName = idx.schema(cacheName);

        return idx.prepareNativeStatement(schemaName, sql);
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void remove(String cacheName, KeyCacheObject key, int partId, CacheObject val, GridCacheVersion ver)
        throws IgniteCheckedException {
        assert key != null;

        if (log.isDebugEnabled())
            log.debug("Remove [cacheName=" + cacheName + ", key=" + key + ", val=" + val + "]");

        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to remove from index (grid is stopping).");

        try {
            CacheObjectContext coctx = cacheObjectContext(cacheName);

            QueryTypeDescriptorImpl desc = typeByValue(cacheName, coctx, key, val, false);

            if (desc == null)
                return;

            idx.remove(cacheName, desc, key, partId, val, ver);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cacheName Cache name.
     * @param clause Clause.
     * @param resType Result type.
     * @param filters Key and value filters.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Key/value rows.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryText(final String cacheName, final String clause,
        final String resType, final IndexingQueryFilter filters) throws IgniteCheckedException {
        checkEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            final GridCacheContext<?, ?> cctx = ctx.cache().internalCache(cacheName).context();

            return executeQuery(GridCacheQueryType.TEXT, clause, cctx,
                new IgniteOutClosureX<GridCloseableIterator<IgniteBiTuple<K, V>>>() {
                    @Override public GridCloseableIterator<IgniteBiTuple<K, V>> applyx() throws IgniteCheckedException {
                        String typeName = typeName(cacheName, resType);
                        String schemaName = idx.schema(cacheName);

                        return idx.queryLocalText(schemaName, clause, typeName, filters);
                    }
                }, true);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Gets types for cache.
     *
     * @param cacheName Cache name.
     * @return Descriptors.
     */
    public Collection<GridQueryTypeDescriptor> types(@Nullable String cacheName) {
        Collection<GridQueryTypeDescriptor> cacheTypes = new ArrayList<>();

        for (Map.Entry<QueryTypeIdKey, QueryTypeDescriptorImpl> e : types.entrySet()) {
            QueryTypeDescriptorImpl desc = e.getValue();

            if (F.eq(e.getKey().cacheName(), cacheName))
                cacheTypes.add(desc);
        }

        return cacheTypes;
    }

    /**
     * Get type descriptor for the given cache and table name.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @return Type (if any).
     */
    @Nullable private QueryTypeDescriptorImpl type(@Nullable String cacheName, String tblName) {
        for (QueryTypeDescriptorImpl type : types.values()) {
            if (F.eq(cacheName, type.cacheName()) && F.eq(tblName, type.tableName()))
                return type;
        }

        return null;
    }

    /**
     * Gets type name for provided cache name and type name if type is still valid.
     *
     * @param cacheName Cache name.
     * @param typeName Type name.
     * @return Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    private String typeName(@Nullable String cacheName, String typeName) throws IgniteCheckedException {
        QueryTypeDescriptorImpl type = typesByName.get(new QueryTypeNameKey(cacheName, typeName));

        if (type == null)
            throw new IgniteCheckedException("Failed to find SQL table for type: " + typeName);

        return type.name();
    }

    /**
     * @param qryType Query type.
     * @param qry Query description.
     * @param cctx Cache context.
     * @param clo Closure.
     * @param complete Complete.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public <R> R executeQuery(GridCacheQueryType qryType, String qry, @Nullable GridCacheContext<?, ?> cctx,
        IgniteOutClosureX<R> clo, boolean complete) throws IgniteCheckedException {
        final long startTime = U.currentTimeMillis();

        Throwable err = null;

        R res = null;

        try {
            res = clo.apply();

            if (res instanceof CacheQueryFuture) {
                CacheQueryFuture fut = (CacheQueryFuture)res;

                err = fut.error();
            }

            return res;
        }
        catch (GridClosureException e) {
            err = e.unwrap();

            throw (IgniteCheckedException)err;
        }
        catch (CacheException | IgniteException e) {
            err = e;

            throw e;
        }
        catch (Exception e) {
            err = e;

            throw new IgniteCheckedException(e);
        }
        finally {
            boolean failed = err != null;

            long duration = U.currentTimeMillis() - startTime;

            if (complete || failed) {
                if (cctx != null)
                    cctx.queries().collectMetrics(qryType, qry, startTime, duration, failed);

                if (log.isTraceEnabled())
                    log.trace("Query execution [startTime=" + startTime + ", duration=" + duration +
                        ", fail=" + failed + ", res=" + res + ']');
            }
        }
    }

    /**
     * Send status message to coordinator node.
     *
     * @param destNodeId Destination node ID.
     * @param opId Operation ID.
     * @param err Error.
     */
    public void sendStatusMessage(UUID destNodeId, UUID opId, SchemaOperationException err) {
        if (log.isDebugEnabled())
            log.debug("Sending schema operation status message [opId=" + opId + ", crdNode=" + destNodeId +
                ", err=" + err + ']');

        try {
            byte[] errBytes = marshalSchemaError(opId, err);

            SchemaOperationStatusMessage msg = new SchemaOperationStatusMessage(opId, errBytes);

            // Messages must go to dedicated schema pool. We cannot push them to query pool because in this case
            // they could be blocked with other query requests.
            ctx.io().sendToGridTopic(destNodeId, TOPIC_SCHEMA, msg, SCHEMA_POOL);
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send schema status response [opId=" + opId + ", destNodeId=" + destNodeId +
                    ", err=" + e + ']');
        }
    }

    /**
     * Process status message.
     *
     * @param msg Status message.
     */
    private void processStatusMessage(SchemaOperationStatusMessage msg) {
        synchronized (stateMux) {
            if (completedOpIds.contains(msg.operationId())) {
                // Received message from a node which joined topology in the middle of operation execution.
                if (log.isDebugEnabled())
                    log.debug("Received status message for completed operation (will ignore) [" +
                        "opId=" + msg.operationId() + ", sndNodeId=" + msg.senderNodeId() + ']');

                return;
            }

            UUID opId = msg.operationId();

            SchemaProposeDiscoveryMessage proposeMsg = activeProposals.get(opId);

            if (proposeMsg != null) {
                SchemaOperation op = schemaOps.get(proposeMsg.schemaName());

                if (op != null && F.eq(op.id(), opId) && op.started() && coordinator().isLocal()) {
                    if (log.isDebugEnabled())
                        log.debug("Received status message [opId=" + msg.operationId() +
                            ", sndNodeId=" + msg.senderNodeId() + ']');

                    op.manager().onNodeFinished(msg.senderNodeId(), unmarshalSchemaError(msg.errorBytes()));

                    return;
                }
            }

            // Put to pending set if operation is not visible/ready yet.
            pendingMsgs.add(msg);

            if (log.isDebugEnabled())
                log.debug("Received status message (added to pending set) [opId=" + msg.operationId() +
                    ", sndNodeId=" + msg.senderNodeId() + ']');
        }
    }

    /**
     * Unwind pending messages for particular operation.
     *
     * @param opId Operation ID.
     * @param mgr Manager.
     */
    private void unwindPendingMessages(UUID opId, SchemaOperationManager mgr) {
        assert Thread.holdsLock(stateMux);

        Iterator<SchemaOperationStatusMessage> it = pendingMsgs.iterator();

        while (it.hasNext()) {
            SchemaOperationStatusMessage msg = it.next();

            if (F.eq(msg.operationId(), opId)) {
                mgr.onNodeFinished(msg.senderNodeId(), unmarshalSchemaError(msg.errorBytes()));

                it.remove();
            }
        }
    }

    /**
     * Marshal schema error.
     *
     * @param err Error.
     * @return Error bytes.
     */
    @Nullable private byte[] marshalSchemaError(UUID opId, @Nullable SchemaOperationException err) {
        if (err == null)
            return null;

        try {
            return U.marshal(marsh, err);
        }
        catch (Exception e) {
            U.warn(log, "Failed to marshal schema operation error [opId=" + opId + ", err=" + err + ']', e);

            try {
                return U.marshal(marsh, new SchemaOperationException("Operation failed, but error cannot be " +
                    "serialized (see local node log for more details) [opId=" + opId + ", nodeId=" +
                    ctx.localNodeId() + ']'));
            }
            catch (Exception e0) {
                assert false; // Impossible situation.

                return null;
            }
        }
    }

    /**
     * Unmarshal schema error.
     *
     * @param errBytes Error bytes.
     * @return Error.
     */
    @Nullable private SchemaOperationException unmarshalSchemaError(@Nullable byte[] errBytes) {
        if (errBytes == null)
            return null;

        try {
            return U.unmarshal(marsh, errBytes, U.resolveClassLoader(ctx.config()));
        }
        catch (Exception e) {
            return new SchemaOperationException("Operation failed, but error cannot be deserialized.");
        }
    }


    /**
     * @return Value object context.
     */
    public CacheQueryObjectValueContext objectContext() {
        return valCtx;
    }

    /**
     * @param ver Version.
     */
    public static void setRequestAffinityTopologyVersion(AffinityTopologyVersion ver) {
        requestTopVer.set(ver);
    }

    /**
     * @return Affinity topology version of the current request.
     */
    public static AffinityTopologyVersion getRequestAffinityTopologyVersion() {
        return requestTopVer.get();
    }

    /**
     * Schema operation.
     */
    private class SchemaOperation {
        /** Original propose msg. */
        private final SchemaProposeDiscoveryMessage proposeMsg;

        /** Next schema operation. */
        private SchemaOperation next;

        /** Operation manager. */
        private SchemaOperationManager mgr;

        /** Finish message. */
        private SchemaFinishDiscoveryMessage finishMsg;

        /** Finish guard. */
        private final AtomicBoolean finishGuard = new AtomicBoolean();

        /**
         * Constructor.
         *
         * @param proposeMsg Original propose message.
         */
        public SchemaOperation(SchemaProposeDiscoveryMessage proposeMsg) {
            this.proposeMsg = proposeMsg;
        }

        /**
         * @return Operation ID.
         */
        public UUID id() {
            return proposeMsg.operation().id();
        }

        /**
         * @return Original propose message.
         */
        public SchemaProposeDiscoveryMessage proposeMessage() {
            return proposeMsg;
        }

        /**
         * @return Next schema operation.
         */
        @Nullable public SchemaOperation next() {
            return next;
        }

        /**
         * @param next Next schema operation.
         */
        public void next(SchemaOperation next) {
            this.next = next;
        }

        /**
         * @param finishMsg Finish message.
         */
        public void finishMessage(SchemaFinishDiscoveryMessage finishMsg) {
            this.finishMsg = finishMsg;
        }

        /**
         * @return {@code True} if finish request already received.
         */
        public boolean hasFinishMessage() {
            return finishMsg != null;
        }

        /**
         * Handle finish message.
         */
        @SuppressWarnings("unchecked")
        public void doFinish() {
            assert started();

            if (!finishGuard.compareAndSet(false, true))
                return;

            final UUID opId = id();
            final String schemaName = proposeMsg.schemaName();

            // Operation might be still in progress on client nodes which are not tracked by coordinator,
            // so we chain to operation future instead of doing synchronous unwind.
            mgr.worker().future().listen(new IgniteInClosure<IgniteInternalFuture>() {
                @Override public void apply(IgniteInternalFuture fut) {
                    synchronized (stateMux) {
                        SchemaOperation op = schemaOps.remove(schemaName);

                        assert op != null;
                        assert F.eq(op.id(), opId);

                        // Chain to the next operation (if any).
                        final SchemaOperation nextOp = op.next();

                        if (nextOp != null) {
                            schemaOps.put(schemaName, nextOp);

                            if (log.isDebugEnabled())
                                log.debug("Next schema change operation started [opId=" + nextOp.id() + ']');

                            assert !nextOp.started();

                            // Cannot execute operation synchronously because it may cause starvation in exchange
                            // thread under load. Hence, moving short-lived operation to separate worker.
                            new IgniteThread(ctx.igniteInstanceName(), "schema-circuit-breaker-" + op.id(),
                                new Runnable() {
                                @Override public void run() {
                                    onSchemaPropose(nextOp.proposeMessage());
                                }
                            }).start();
                        }
                    }
                }
            });
        }

        /**
         * Unwind operation queue and get tail operation.
         *
         * @return Tail operation.
         */
        public SchemaOperation unwind() {
            if (next == null)
                return this;
            else
                return next.unwind();
        }

        /**
         * Whether operation started.
         *
         * @return {@code True} if started.
         */
        public boolean started() {
            return mgr != null;
        }

        /**
         * @return Operation manager.
         */
        public SchemaOperationManager manager() {
            return mgr;
        }

        /**
         * @param mgr Operation manager.
         */
        public void manager(SchemaOperationManager mgr) {
            assert this.mgr == null;

            this.mgr = mgr;
        }
    }
}
