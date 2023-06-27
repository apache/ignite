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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryProperties;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.Query;
import org.apache.ignite.internal.processors.query.calcite.QueryRegistry;
import org.apache.ignite.internal.processors.query.calcite.QueryState;
import org.apache.ignite.internal.processors.query.calcite.RootQuery;
import org.apache.ignite.internal.processors.query.calcite.RunningFragment;
import org.apache.ignite.internal.processors.query.calcite.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.GlobalMemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.IoTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.MemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.NoOpIoTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.NoOpMemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.PerformanceStatisticsIoTracker;
import org.apache.ignite.internal.processors.query.calcite.message.ErrorMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartRequest;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartResponse;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.RemoteException;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.CacheKey;
import org.apache.ignite.internal.processors.query.calcite.prepare.DdlPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.ExplainPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.FieldsMetadataImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.FragmentPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCache;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.ConvertingClosableIterator;
import org.apache.ignite.internal.processors.query.calcite.util.ListFieldsQueryCursor;
import org.apache.ignite.internal.processors.query.running.HeavyQueriesTracker;
import org.apache.ignite.internal.processors.security.SecurityUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.processors.query.calcite.externalize.RelJsonReader.fromJson;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
public class ExecutionServiceImpl<Row> extends AbstractService implements ExecutionService<Row> {
    /** */
    private final DiscoveryEventListener discoLsnr;

    /** */
    private UUID locNodeId;

    /** */
    private GridKernalContext ctx;

    /** */
    private GridEventStorageManager evtMgr;

    /** */
    private GridCachePartitionExchangeManager<?, ?> exchangeMgr;

    /** */
    private CacheObjectValueContext objValCtx;

    /** */
    private QueryPlanCache qryPlanCache;

    /** */
    private SchemaHolder schemaHolder;

    /** */
    private QueryTaskExecutor taskExecutor;

    /** */
    private FailureProcessor failureProcessor;

    /** */
    private PerformanceStatisticsProcessor perfStatProc;

    /** */
    private AffinityService partSvc;

    /** */
    private MailboxRegistry mailboxRegistry;

    /** */
    private MappingService mappingSvc;

    /** */
    private MessageService msgSvc;

    /** */
    private ExchangeService exchangeSvc;

    /** */
    private PrepareServiceImpl prepareSvc;

    /** */
    private ClosableIteratorsHolder iteratorsHolder;

    /** */
    private QueryRegistry qryReg;

    /** */
    private final RowHandler<Row> handler;

    /** */
    private DdlCommandHandler ddlCmdHnd;

    /** */
    private CalciteQueryEngineConfiguration cfg;

    /** */
    private MemoryTracker memoryTracker;

    /**
     * @param ctx Kernal.
     */
    public ExecutionServiceImpl(GridKernalContext ctx, RowHandler<Row> handler) {
        super(ctx);
        this.handler = handler;

        discoLsnr = (e, c) -> onNodeLeft(e.eventNode().id());
    }

    /**
     * @param locNodeId Local node ID.
     */
    public void localNodeId(UUID locNodeId) {
        this.locNodeId = locNodeId;
    }

    /**
     * @return Local node ID.
     */
    public UUID localNodeId() {
        return locNodeId;
    }

    /**
     * @param qryPlanCache Query cache.
     */
    public void queryPlanCache(QueryPlanCache qryPlanCache) {
        this.qryPlanCache = qryPlanCache;
    }

    /**
     * @return Query cache.
     */
    public QueryPlanCache queryPlanCache() {
        return qryPlanCache;
    }

    /**
     * @param schemaHolder Schema holder.
     */
    public void schemaHolder(SchemaHolder schemaHolder) {
        this.schemaHolder = schemaHolder;
    }

    /**
     * @return Schema holder.
     */
    public SchemaHolder schemaHolder() {
        return schemaHolder;
    }

    /**
     * @param taskExecutor Task executor.
     */
    public void taskExecutor(QueryTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    /**
     * @return Task executor.
     */
    public QueryTaskExecutor taskExecutor() {
        return taskExecutor;
    }

    /**
     * @param failureProcessor Failure processor.
     */
    public void failureProcessor(FailureProcessor failureProcessor) {
        this.failureProcessor = failureProcessor;
    }

    /**
     * @return Failure processor.
     */
    public FailureProcessor failureProcessor() {
        return failureProcessor;
    }

    /**
     * @param perfStatProc Performance statistics processor.
     */
    public void performanceStatisticsProcessor(PerformanceStatisticsProcessor perfStatProc) {
        this.perfStatProc = perfStatProc;
    }

    /**
     * @param partSvc Partition service.
     */
    public void partitionService(AffinityService partSvc) {
        this.partSvc = partSvc;
    }

    /**
     * @return Partition service.
     */
    public AffinityService partitionService() {
        return partSvc;
    }

    /**
     * @param mailboxRegistry Mailbox registry.
     */
    public void mailboxRegistry(MailboxRegistry mailboxRegistry) {
        this.mailboxRegistry = mailboxRegistry;
    }

    /**
     * @return Mailbox registry.
     */
    public MailboxRegistry mailboxRegistry() {
        return mailboxRegistry;
    }

    /**
     * @param mappingSvc Mapping service.
     */
    public void mappingService(MappingService mappingSvc) {
        this.mappingSvc = mappingSvc;
    }

    /**
     * @return Mapping service.
     */
    public MappingService mappingService() {
        return mappingSvc;
    }

    /**
     * @param msgSvc Message service.
     */
    public void messageService(MessageService msgSvc) {
        this.msgSvc = msgSvc;
    }

    /**
     * @return Message service.
     */
    public MessageService messageService() {
        return msgSvc;
    }

    /**
     * @param exchangeSvc Exchange service.
     */
    public void exchangeService(ExchangeService exchangeSvc) {
        this.exchangeSvc = exchangeSvc;
    }

    /**
     * @param prepareSvc Prepare service.
     */
    public void prepareService(PrepareServiceImpl prepareSvc) {
        this.prepareSvc = prepareSvc;
    }

    /**
     * @return Exchange service.
     */
    public ExchangeService exchangeService() {
        return exchangeSvc;
    }

    /**
     * @param evtMgr Event manager.
     */
    public void eventManager(GridEventStorageManager evtMgr) {
        this.evtMgr = evtMgr;
    }

    /**
     * @return Event manager.
     */
    public GridEventStorageManager eventManager() {
        return evtMgr;
    }

    /**
     * @param exchangeMgr Exchange manager.
     */
    public void exchangeManager(GridCachePartitionExchangeManager<?, ?> exchangeMgr) {
        this.exchangeMgr = exchangeMgr;
    }

    /**
     * @param objValCtx Cache object value context.
     */
    public void cacheObjectValueContext(CacheObjectValueContext objValCtx) {
        this.objValCtx = objValCtx;
    }

    /**
     * @return Exchange manager.
     */
    public GridCachePartitionExchangeManager<?, ?> exchangeManager() {
        return exchangeMgr;
    }

    /**
     * @param iteratorsHolder Iterators holder.
     */
    public void iteratorsHolder(ClosableIteratorsHolder iteratorsHolder) {
        this.iteratorsHolder = iteratorsHolder;
    }

    /**
     * @return Iterators holder.
     */
    public ClosableIteratorsHolder iteratorsHolder() {
        return iteratorsHolder;
    }

    /** */
    public void queryRegistry(QueryRegistry qryReg) {
        this.qryReg = qryReg;
    }

    /** */
    public MemoryTracker memoryTracker() {
        return memoryTracker;
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        this.ctx = ctx;
        localNodeId(ctx.localNodeId());
        exchangeManager(ctx.cache().context().exchange());
        cacheObjectValueContext(ctx.query().objectContext());
        eventManager(ctx.event());
        performanceStatisticsProcessor(ctx.performanceStatistics());
        iteratorsHolder(new ClosableIteratorsHolder(log));

        CalciteQueryProcessor proc = Objects.requireNonNull(
            Commons.lookupComponent(ctx, CalciteQueryProcessor.class));

        queryPlanCache(proc.queryPlanCache());
        schemaHolder(proc.schemaHolder());
        taskExecutor(proc.taskExecutor());
        failureProcessor(proc.failureProcessor());
        partitionService(proc.affinityService());
        mailboxRegistry(proc.mailboxRegistry());
        mappingService(proc.mappingService());
        messageService(proc.messageService());
        exchangeService(proc.exchangeService());
        queryRegistry(proc.queryRegistry());
        prepareService(proc.prepareService());

        ddlCmdHnd = new DdlCommandHandler(ctx.query(), ctx.cache(), ctx.security(), () -> schemaHolder().schema(null));

        cfg = proc.config();

        memoryTracker = cfg.getGlobalMemoryQuota() > 0 ? new GlobalMemoryTracker(cfg.getGlobalMemoryQuota()) :
            NoOpMemoryTracker.INSTANCE;

        init();
    }

    /** {@inheritDoc} */
    @Override public void init() {
        messageService().register((n, m) -> onMessage(n, (QueryStartRequest)m), MessageType.QUERY_START_REQUEST);
        messageService().register((n, m) -> onMessage(n, (QueryStartResponse)m), MessageType.QUERY_START_RESPONSE);
        messageService().register((n, m) -> onMessage(n, (ErrorMessage)m), MessageType.QUERY_ERROR_MESSAGE);

        eventManager().addDiscoveryEventListener(discoLsnr, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);

        iteratorsHolder().init();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        eventManager().removeDiscoveryEventListener(discoLsnr, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);

        iteratorsHolder().tearDown();
    }

    /** */
    protected AffinityTopologyVersion topologyVersion() {
        return exchangeManager().readyAffinityVersion();
    }

    /** */
    private BaseQueryContext createQueryContext(Context parent, @Nullable String schema) {
        return BaseQueryContext.builder()
            .parentContext(parent)
            .frameworkConfig(
                Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                    .defaultSchema(schemaHolder().schema(schema))
                    .build()
            )
            .logger(log)
            .build();
    }

    /** */
    private QueryPlan prepareFragment(BaseQueryContext ctx, String jsonFragment) {
        return new FragmentPlan(jsonFragment, fromJson(ctx, jsonFragment));
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> executePlan(
        RootQuery<Row> qry,
        QueryPlan plan
    ) {
        switch (plan.type()) {
            case DML:
                ListFieldsQueryCursor<?> cur = mapAndExecutePlan(
                    qry,
                    (MultiStepPlan)plan
                );

                cur.iterator().hasNext();

                return cur;

            case QUERY:
                return mapAndExecutePlan(
                    qry,
                    (MultiStepPlan)plan
                );

            case EXPLAIN:
                return executeExplain(qry, (ExplainPlan)plan);

            case DDL:
                return executeDdl(qry, (DdlPlan)plan);

            default:
                throw new AssertionError("Unexpected plan type: " + plan);
        }
    }

    /** */
    private FieldsQueryCursor<List<?>> executeDdl(RootQuery<Row> qry, DdlPlan plan) {
        try {
            ddlCmdHnd.handle(qry.id(), plan.command());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSQLException("Failed to execute DDL statement [stmt=" + qry.sql() +
                ", err=" + e.getMessage() + ']', e);
        }

        if (plan.command() instanceof CreateTableCommand
            && ((CreateTableCommand)plan.command()).insertStatement() != null) {
            RootQuery<Row> insQry = qry.childQuery(schemaHolder.schema(qry.context().schemaName()));

            qryReg.register(insQry);

            SqlInsert insertStmt = ((CreateTableCommand)plan.command()).insertStatement();

            QueryPlan dmlPlan = prepareSvc.prepareSingle(insertStmt, insQry.planningContext());

            return executePlan(insQry, dmlPlan);
        }
        else {
            QueryCursorImpl<List<?>> resCur = new QueryCursorImpl<>(Collections.singletonList(
                Collections.singletonList(0L)), null, false, false);

            IgniteTypeFactory typeFactory = qry.context().typeFactory();

            resCur.fieldsMeta(new FieldsMetadataImpl(RelOptUtil.createDmlRowType(
                SqlKind.INSERT, typeFactory), null).queryFieldsMetadata(typeFactory));

            return resCur;
        }
    }

    /** */
    private ListFieldsQueryCursor<?> mapAndExecutePlan(
        RootQuery<Row> qry,
        MultiStepPlan plan
    ) {
        qry.mapping();

        MappingQueryContext mapCtx = Commons.mapContext(locNodeId, topologyVersion());
        plan.init(mappingSvc, mapCtx);

        List<Fragment> fragments = plan.fragments();

        // Local execution
        Fragment fragment = F.first(fragments);

        if (U.assertionsEnabled()) {
            assert fragment != null;

            FragmentMapping mapping = plan.mapping(fragment);

            assert mapping != null;

            List<UUID> nodes = mapping.nodeIds();

            assert nodes != null && nodes.size() == 1 && F.first(nodes).equals(localNodeId());
        }

        FragmentDescription fragmentDesc = new FragmentDescription(
            fragment.fragmentId(),
            plan.mapping(fragment),
            plan.target(fragment),
            plan.remotes(fragment));

        ExecutionContext<Row> ectx = new ExecutionContext<>(
            qry.context(),
            taskExecutor(),
            qry.id(),
            locNodeId,
            locNodeId,
            mapCtx.topologyVersion(),
            fragmentDesc,
            handler,
            qry.createMemoryTracker(memoryTracker, cfg.getQueryMemoryQuota()),
            createIoTracker(locNodeId, qry.localQueryId()),
            Commons.parametersMap(qry.parameters()));

        Node<Row> node = new LogicalRelImplementor<>(ectx, partitionService(), mailboxRegistry(),
            exchangeService(), failureProcessor()).go(fragment.root());

        qry.run(ectx, plan, node);

        Map<UUID, Long> fragmentsPerNode = fragments.stream()
            .skip(1)
            .flatMap(f -> f.mapping().nodeIds().stream())
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        // Start remote execution.
        for (int i = 1; i < fragments.size(); i++) {
            fragment = fragments.get(i);
            fragmentDesc = new FragmentDescription(
                fragment.fragmentId(),
                plan.mapping(fragment),
                plan.target(fragment),
                plan.remotes(fragment));

            Throwable ex = null;
            byte[] parametersMarshalled = null;

            for (UUID nodeId : fragmentDesc.nodeIds()) {
                if (ex != null)
                    qry.onResponse(nodeId, fragment.fragmentId(), ex);
                else {
                    try {
                        QueryStartRequest req = new QueryStartRequest(
                            qry.id(),
                            qry.localQueryId(),
                            qry.context().schemaName(),
                            fragment.serialized(),
                            ectx.topologyVersion(),
                            fragmentDesc,
                            fragmentsPerNode.get(nodeId).intValue(),
                            qry.parameters(),
                            parametersMarshalled
                        );

                        messageService().send(nodeId, req);

                        // Avoid marshaling of the same parameters for other nodes.
                        if (parametersMarshalled == null)
                            parametersMarshalled = req.parametersMarshalled();
                    }
                    catch (Throwable e) {
                        qry.onResponse(nodeId, fragment.fragmentId(), ex = e);
                    }
                }
            }
        }

        QueryProperties qryProps = qry.context().unwrap(QueryProperties.class);

        Function<Object, Object> fieldConverter = (qryProps == null || qryProps.keepBinary()) ? null :
            o -> CacheObjectUtils.unwrapBinaryIfNeeded(objValCtx, o, false, true, null);

        HeavyQueriesTracker.ResultSetChecker resultSetChecker = ctx.query().runningQueryManager()
            .heavyQueriesTracker().resultSetChecker(qry);

        Function<List<Object>, List<Object>> rowConverter;

        // Fire EVT_CACHE_QUERY_OBJECT_READ on initiator node before return result to cursor.
        if (qryProps != null && qryProps.cacheName() != null && evtMgr.isRecordable(EVT_CACHE_QUERY_OBJECT_READ)) {
            ClusterNode locNode = ctx.discovery().localNode();
            UUID subjId = SecurityUtils.securitySubjectId(ctx);

            rowConverter = row -> {
                evtMgr.record(new CacheQueryReadEvent<>(
                    locNode,
                    "SQL fields query result set row read.",
                    EVT_CACHE_QUERY_OBJECT_READ,
                    CacheQueryType.SQL_FIELDS.name(),
                    qryProps.cacheName(),
                    null,
                    qry.sql(),
                    null,
                    null,
                    qry.parameters(),
                    subjId,
                    null,
                    null,
                    null,
                    null,
                    row));

                resultSetChecker.checkOnFetchNext();

                return row;
            };
        }
        else {
            rowConverter = row -> {
                resultSetChecker.checkOnFetchNext();

                return row;
            };
        }

        Iterator<List<?>> it = new ConvertingClosableIterator<>(iteratorsHolder().iterator(qry.iterator()), ectx,
            fieldConverter, rowConverter, resultSetChecker::checkOnClose);

        return new ListFieldsQueryCursor<>(plan, it, ectx);
    }

    /** */
    private FieldsQueryCursor<List<?>> executeExplain(RootQuery<Row> qry, ExplainPlan plan) {
        QueryCursorImpl<List<?>> cur = new QueryCursorImpl<>(singletonList(singletonList(plan.plan())));
        cur.fieldsMeta(plan.fieldsMeta().queryFieldsMetadata(Commons.typeFactory()));

        return cur;
    }

    /** */
    private void executeFragment(Query<Row> qry, FragmentPlan plan, ExecutionContext<Row> ectx) {
        UUID origNodeId = ectx.originatingNodeId();

        Outbox<Row> node = new LogicalRelImplementor<>(
            ectx,
            partitionService(),
            mailboxRegistry(),
            exchangeService(),
            failureProcessor()
        )
            .go(plan.root());

        qry.addFragment(new RunningFragment<>(plan.root(), node, ectx));

        node.init();

        if (!qry.isExchangeWithInitNodeStarted(ectx.fragmentId())) {
            try {
                messageService().send(origNodeId, new QueryStartResponse(qry.id(), ectx.fragmentId()));
            }
            catch (IgniteCheckedException e) {
                IgniteException wrpEx = new IgniteException("Failed to send reply. [nodeId=" + origNodeId + ']', e);

                throw wrpEx;
            }
        }
    }

    /** */
    private void onMessage(UUID nodeId, final QueryStartRequest msg) {
        assert nodeId != null && msg != null;

        try {
            Query<Row> qry = (Query<Row>)qryReg.register(
                new Query<>(
                    msg.queryId(),
                    nodeId,
                    null,
                    exchangeSvc,
                    (q, ex) -> qryReg.unregister(q.id(), ex),
                    log,
                    msg.totalFragmentsCount()
                )
            );

            final BaseQueryContext qctx = createQueryContext(Contexts.empty(), msg.schema());

            QueryPlan qryPlan = queryPlanCache().queryPlan(
                new CacheKey(msg.schema(), msg.root()),
                () -> prepareFragment(qctx, msg.root())
            );

            assert qryPlan.type() == QueryPlan.Type.FRAGMENT;

            ExecutionContext<Row> ectx = new ExecutionContext<>(
                qctx,
                taskExecutor(),
                msg.queryId(),
                locNodeId,
                nodeId,
                msg.topologyVersion(),
                msg.fragmentDescription(),
                handler,
                qry.createMemoryTracker(memoryTracker, cfg.getQueryMemoryQuota()),
                createIoTracker(nodeId, msg.originatingQryId()),
                Commons.parametersMap(msg.parameters())
            );

            executeFragment(qry, (FragmentPlan)qryPlan, ectx);
        }
        catch (Throwable ex) {
            U.error(log, "Failed to start query fragment ", ex);

            mailboxRegistry.outboxes(msg.queryId(), msg.fragmentId(), -1)
                .forEach(Outbox::close);
            mailboxRegistry.inboxes(msg.queryId(), msg.fragmentId(), -1)
                .forEach(Inbox::close);

            try {
                messageService().send(nodeId, new QueryStartResponse(msg.queryId(), msg.fragmentId(), ex));
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Error occurred during send error message: " + X.getFullStackTrace(e));

                IgniteException wrpEx = new IgniteException("Error occurred during send error message", e);

                e.addSuppressed(ex);

                Query<Row> qry = (Query<Row>)qryReg.query(msg.queryId());

                qry.cancel();

                throw wrpEx;
            }

            throw ex;
        }
    }

    /** */
    private void onMessage(UUID nodeId, QueryStartResponse msg) {
        assert nodeId != null && msg != null;

        Query<?> qry = qryReg.query(msg.queryId());

        if (qry != null) {
            assert qry instanceof RootQuery : "Unexpected query object: " + qry;

            ((RootQuery<Row>)qry).onResponse(nodeId, msg.fragmentId(), msg.error());
        }
    }

    /** */
    private void onMessage(UUID nodeId, ErrorMessage msg) {
        assert nodeId != null && msg != null;

        Query<?> qry = qryReg.query(msg.queryId());

        if (qry != null && qry.state() != QueryState.CLOSED) {
            assert qry instanceof RootQuery : "Unexpected query object: " + qry;

            Exception e = new RemoteException(nodeId, msg.queryId(), msg.fragmentId(), msg.error());

            if (X.hasCause(msg.error(), QueryCancelledException.class)) {
                e = new IgniteSQLException(
                    "The query was cancelled while executing.",
                    IgniteQueryErrorCode.QUERY_CANCELED,
                    e
                );
            }

            ((RootQuery<Row>)qry).onError(e);
        }
    }

    /** */
    private void onNodeLeft(UUID nodeId) {
        qryReg.runningQueries()
            .forEach((qry) -> qry.onNodeLeft(nodeId));
    }

    /** */
    private IoTracker createIoTracker(UUID originatingNodeId, long originatingQryId) {
        return perfStatProc.enabled() ?
            new PerformanceStatisticsIoTracker(perfStatProc, originatingNodeId, originatingQryId) :
            NoOpIoTracker.INSTANCE;
    }
}
