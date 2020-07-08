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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.ValidationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryCancellable;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.RootNode;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartRequest;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartResponse;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.PartitionService;
import org.apache.ignite.internal.processors.query.calcite.prepare.CacheKey;
import org.apache.ignite.internal.processors.query.calcite.prepare.CalciteQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.calcite.prepare.ExplainPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepDmlPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepQueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCache;
import org.apache.ignite.internal.processors.query.calcite.prepare.Splitter;
import org.apache.ignite.internal.processors.query.calcite.prepare.ValidationResult;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.ListFieldsQueryCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.processors.query.calcite.externalize.RelJsonReader.fromJson;
import static org.apache.ignite.internal.processors.query.calcite.externalize.RelJsonWriter.toJson;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
public class ExecutionServiceImpl<Row> extends AbstractService implements ExecutionService {
    /** */
    private final DiscoveryEventListener discoLsnr;

    /** */
    private UUID locNodeId;

    /** */
    private GridEventStorageManager evtMgr;

    /** */
    private GridCachePartitionExchangeManager<?,?> exchangeMgr;

    /** */
    private QueryPlanCache qryPlanCache;

    /** */
    private SchemaHolder schemaHolder;

    /** */
    private QueryTaskExecutor taskExecutor;

    /** */
    private FailureProcessor failureProcessor;

    /** */
    private PartitionService partSvc;

    /** */
    private MailboxRegistry mailboxRegistry;

    /** */
    private MappingService mappingSvc;

    /** */
    private MessageService msgSvc;

    /** */
    private ExchangeService exchangeSvc;

    /** */
    private ClosableIteratorsHolder iteratorsHolder;

    /** */
    private final Map<UUID, QueryInfo> running;

    /** */
    private final RowHandler<Row> handler;

    /**
     * @param ctx Kernal.
     */
    public ExecutionServiceImpl(GridKernalContext ctx, RowHandler<Row> handler) {
        super(ctx);
        this.handler = handler;

        discoLsnr = (e, c) -> onNodeLeft(e.eventNode().id());
        running = new ConcurrentHashMap<>();
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
     * @param partSvc Partition service.
     */
    public void partitionService(PartitionService partSvc) {
        this.partSvc = partSvc;
    }

    /**
     * @return Partition service.
     */
    public PartitionService partitionService() {
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
    public void exchangeManager(GridCachePartitionExchangeManager<?,?> exchangeMgr) {
        this.exchangeMgr = exchangeMgr;
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

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> executeQuery(
        @Nullable QueryContext ctx,
        String schema,
        String qry,
        Object[] params
    ) {
        PlanningContext pctx = createContext(ctx, schema, qry, params);

        List<QueryPlan> qryPlans = prepareQueryPlan(pctx);

        return executePlans(qryPlans, pctx);
    }

    /**
     * Executes prepared plans.
     * @param qryPlans Query plans.
     * @param pctx Query context.
     * @return List of query result cursors.
     */
    @NotNull public List<FieldsQueryCursor<List<?>>> executePlans(
        Collection<QueryPlan> qryPlans,
        PlanningContext pctx
    ) {
        List<FieldsQueryCursor<List<?>>> cursors = new ArrayList<>(qryPlans.size());

        for (QueryPlan plan : qryPlans) {
            UUID qryId = UUID.randomUUID();

            FieldsQueryCursor<List<?>> cur = executePlan(qryId, pctx, plan);

            cursors.add(cur);
        }

        return cursors;
    }

    /** {@inheritDoc} */
    @Override public void cancelQuery(UUID qryId) {
        QueryInfo info = running.remove(qryId);

        if (info != null)
            info.cancel();
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        localNodeId(ctx.localNodeId());
        exchangeManager(ctx.cache().context().exchange());
        eventManager(ctx.event());
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

        init();
     }

    /** {@inheritDoc} */
    @Override public void init() {
        messageService().register((n,m) -> onMessage(n, (QueryStartRequest) m), MessageType.QUERY_START_REQUEST);
        messageService().register((n,m) -> onMessage(n, (QueryStartResponse) m), MessageType.QUERY_START_RESPONSE);

        eventManager().addDiscoveryEventListener(discoLsnr, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);

        iteratorsHolder().init();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        eventManager().removeDiscoveryEventListener(discoLsnr, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);

        running.clear();

        iteratorsHolder().tearDown();
    }

    /** */
    protected AffinityTopologyVersion topologyVersion() {
        return exchangeManager().readyAffinityVersion();
    }

    /** */
    private PlanningContext createContext(
        @Nullable QueryContext qryCtx,
        @Nullable String schemaName,
        String qry,
        Object[] params
    ) {
        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE,
            DistributionTraitDef.INSTANCE
        };

        return PlanningContext.builder()
            .localNodeId(localNodeId())
            .parentContext(Commons.convert(qryCtx))
            .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schemaName != null
                    ? schemaHolder().schema().getSubSchema(schemaName)
                    : schemaHolder().schema())
                .traitDefs(traitDefs)
                .build())
            .query(qry)
            .parameters(params)
            .topologyVersion(topologyVersion())
            .logger(log)
            .build();
    }

    /** */
    private PlanningContext createContext(
        @Nullable String schemaName,
        UUID originatingNodeId,
        AffinityTopologyVersion topVer
    ) {
        // TODO pass to context user locale and timezone.

        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE,
            DistributionTraitDef.INSTANCE
        };

        return PlanningContext.builder()
            .localNodeId(localNodeId())
            .originatingNodeId(originatingNodeId)
            .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schemaName != null
                    ? schemaHolder().schema().getSubSchema(schemaName)
                    : schemaHolder().schema())
                .traitDefs(traitDefs)
                .build())
            .topologyVersion(topVer)
            .logger(log)
            .build();
    }

    /** */
    private List<QueryPlan> prepareQueryPlan(PlanningContext ctx) {
        return queryPlanCache().queryPlan(ctx, new CacheKey(ctx.schemaName(), ctx.query()), this::prepare0);
    }

    /** */
    private List<QueryPlan> prepare0(PlanningContext ctx) {
        try {
            String qry = ctx.query();

            assert qry != null;

            // Parse query.
            SqlNode sqlNode = ctx.planner().parse(qry);

            if (single(sqlNode))
                return singletonList(prepareSingle(sqlNode, ctx));

            List<SqlNode> nodes = ((SqlNodeList) sqlNode).getList();
            List<QueryPlan> res = new ArrayList<>(nodes.size());

            for (SqlNode node : nodes)
                res.add(prepareSingle(node, ctx));

            return res;
        }
        catch (IgniteSQLException e) {
            throw e;
        }
        catch (SqlParseException e) {
            throw new IgniteSQLException("Failed to parse query.", IgniteQueryErrorCode.PARSING, e);
        }
        catch (ValidationException e) {
            throw new IgniteSQLException("Failed to validate query.", IgniteQueryErrorCode.PARSING, e);
        }
        catch (Exception e) {
            throw new IgniteSQLException("Failed to plan query.", IgniteQueryErrorCode.UNKNOWN, e);
        }
    }

    /** */
    private QueryPlan prepareSingle(SqlNode sqlNode, PlanningContext ctx) throws ValidationException {
        assert single(sqlNode);

        ctx.planner().reset();

        switch (sqlNode.getKind()) {
            case SELECT:
            case ORDER_BY:
            case UNION:
                return prepareQuery(sqlNode, ctx);

            case INSERT:
            case DELETE:
            case UPDATE:
                return prepareDml(sqlNode, ctx);

            case EXPLAIN:
                return prepareExplain(sqlNode, ctx);

            default:
                throw new IgniteSQLException("Unsupported operation [" +
                    "sqlNodeKind=" + sqlNode.getKind() + "; " +
                    "querySql=\"" + ctx.query() + "\"]", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }
    }

    /** */
    private QueryPlan prepareQuery(SqlNode sqlNode, PlanningContext ctx) {
        IgnitePlanner planner = ctx.planner();

        // Validate
        ValidationResult validated = planner.validateAndGetTypeMetadata(sqlNode);

        sqlNode = validated.sqlNode();

        IgniteRel igniteRel = optimize(sqlNode, planner);

        // Split query plan to query fragments.
        List<Fragment> fragments = new Splitter().go(igniteRel);

        return new MultiStepQueryPlan(fragments, fieldsMetadata(ctx, validated.dataType(), validated.origins()));
    }

    /** */
    private QueryPlan prepareDml(SqlNode sqlNode, PlanningContext ctx) throws ValidationException {
        IgnitePlanner planner = ctx.planner();

        // Validate
        sqlNode = planner.validate(sqlNode);

        // Convert to Relational operators graph
        IgniteRel igniteRel = optimize(sqlNode, planner);

        // Split query plan to query fragments.
        List<Fragment> fragments = new Splitter().go(igniteRel);

        return new MultiStepDmlPlan(fragments, fieldsMetadata(ctx, igniteRel.getRowType(), null));
    }

    /** */
    private IgniteRel optimize(SqlNode sqlNode, IgnitePlanner planner) {
        // Convert to Relational operators graph
        RelRoot root = planner.rel(sqlNode);

        RelNode rel = root.project();

        // Transformation chain
        rel = planner.transform(PlannerPhase.HEURISTIC_OPTIMIZATION, rel.getTraitSet(), rel);

        RelTraitSet desired = rel.getCluster().traitSet()
            .replace(IgniteConvention.INSTANCE)
            .replace(IgniteDistributions.single())
            .replace(root.collation == null ? RelCollations.EMPTY : root.collation)
            .simplify();

        return planner.transform(PlannerPhase.OPTIMIZATION, desired, rel);
    }

    /** */
    private QueryPlan prepareExplain(SqlNode explain, PlanningContext ctx) throws ValidationException {
        IgnitePlanner planner = ctx.planner();

        SqlNode sql = ((SqlExplain)explain).getExplicandum();

        // Validate
        explain = planner.validate(sql);

        // Convert to Relational operators graph
        IgniteRel igniteRel = optimize(explain, planner);

        List<GridQueryFieldMetadata> meta = buildExplainColumnMeta(ctx);

        String plan = RelOptUtil.toString(igniteRel, SqlExplainLevel.ALL_ATTRIBUTES);

        return new ExplainPlan(plan, meta);
    }

    /** */
    private List<GridQueryFieldMetadata> buildExplainColumnMeta(PlanningContext ctx) {
        IgniteTypeFactory factory = ctx.typeFactory();
        RelDataType planStrDataType =
            factory.createSqlType(SqlTypeName.VARCHAR, RelDataType.PRECISION_NOT_SPECIFIED);
        T2<String, RelDataType> planField = new T2<>(ExplainPlan.PLAN_COL_NAME, planStrDataType);
        RelDataType planDataType = factory.createStructType(singletonList(planField));

        return fieldsMetadata(ctx, planDataType, null);
    }

    /** */
    private FieldsQueryCursor<List<?>> executePlan(UUID qryId, PlanningContext pctx, QueryPlan plan) {
        switch (plan.type()) {
            case DML:
                // TODO a barrier between previous operation and this one
            case QUERY:
                return executeQuery(qryId, (MultiStepPlan) plan, pctx);

            case EXPLAIN:
                return executeExplain(plan);

            default:
                throw new AssertionError("Unexpected plan type: " + plan);
        }
    }

    /** */
    private FieldsQueryCursor<List<?>> executeQuery(UUID qryId, MultiStepPlan plan, PlanningContext pctx) {
        plan.init(mappingService(), pctx);

        List<Fragment> fragments = plan.fragments();

        // Local execution
        Fragment fragment = F.first(fragments);
        NodesMapping mapping = plan.fragmentMapping(fragment);

        if (U.assertionsEnabled()) {
            assert fragment != null;
            assert mapping != null;

            List<UUID> nodes = mapping.nodes();

            assert nodes != null && nodes.size() == 1 && F.first(nodes).equals(pctx.localNodeId());
        }

        FragmentDescription fragmentDesc = new FragmentDescription(
            fragment.fragmentId(),
            mapping.partitions(pctx.localNodeId()),
            mapping.assignments().size(),
            plan.targetMapping(fragment),
            plan.remoteSources(fragment)
        );

        ExecutionContext<Row> ectx = new ExecutionContext<>(
            taskExecutor(),
            pctx,
            qryId,
            fragmentDesc,
            handler,
            Commons.parametersMap(pctx.parameters()));

        Node<Row> node = new LogicalRelImplementor<>(ectx, partitionService(), mailboxRegistry(),
            exchangeService(), failureProcessor()).go(fragment.root());

        QueryInfo info = new QueryInfo(ectx, plan, node);

        // register query
        register(info);

        // start remote execution
        if (fragments.size() > 1) {
            for (int i = 1; i < fragments.size(); i++) {
                Fragment fragment0 = fragments.get(i);
                NodesMapping mapping0 = plan.fragmentMapping(fragment0);

                boolean error = false;

                for (UUID nodeId : mapping0.nodes()) {
                    if (error)
                        info.onResponse(nodeId, fragment0.fragmentId(), new QueryCancelledException());
                    else {
                        try {
                            FragmentDescription fragmentDesc0 = new FragmentDescription(
                                fragment0.fragmentId(),
                                mapping0.partitions(nodeId),
                                mapping0.assignments().size(),
                                plan.targetMapping(fragment0),
                                plan.remoteSources(fragment0)
                            );

                            QueryStartRequest req = new QueryStartRequest(
                                qryId,
                                pctx.schemaName(),
                                toJson(fragment0.root()),
                                pctx.topologyVersion(),
                                fragmentDesc0,
                                pctx.parameters());

                            messageService().send(nodeId, req);
                        }
                        catch (Exception e) {
                            info.onResponse(nodeId, fragment0.fragmentId(), e);
                            error = true;
                        }
                    }
                }

                if (error) {
                    info.awaitAllReplies();

                    throw new AssertionError(); // Previous call must throw an exception
                }
            }
        }

        return new ListFieldsQueryCursor<>(plan, info.iterator(), ectx);
    }

    /** */
    private FieldsQueryCursor<List<?>> executeExplain(QueryPlan plan) {
        ExplainPlan explainPlan = (ExplainPlan)plan;
        List<List<?>> resSet = singletonList(singletonList(explainPlan.plan()));

        QueryCursorImpl<List<?>> cur = new QueryCursorImpl<>(resSet);
        cur.fieldsMeta(explainPlan.fieldsMeta());

        return cur;
    }

    /** */
    private void register(QueryInfo info) {
        UUID qryId = info.ctx.queryId();
        PlanningContext pctx = info.ctx.planningContext();

        running.put(qryId, info);

        GridQueryCancel qryCancel = pctx.queryCancel();

        if (qryCancel == null)
            return;

        try {
            qryCancel.add(info);
        }
        catch (QueryCancelledException e) {
            running.remove(qryId);

            throw new IgniteSQLException(e.getMessage(), IgniteQueryErrorCode.QUERY_CANCELED);
        }
    }

    /** */
    private List<GridQueryFieldMetadata> fieldsMetadata(
        PlanningContext ctx,
        RelDataType type,
        @Nullable List<List<String>> origins
    ) {
        List<RelDataTypeField> fields = type.getFieldList();

        assert origins == null || fields.size() == origins.size();

        ImmutableList.Builder<GridQueryFieldMetadata> b = ImmutableList.builder();

        IgniteTypeFactory typeFactory = ctx.typeFactory();

        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            List<String> origin = origins != null ? origins.get(i) : null;

            b.add(new CalciteQueryFieldMetadata(
                F.isEmpty(origin) ? null : origin.get(0),
                F.isEmpty(origin) ? null : origin.get(1),
                field.getName(),
                String.valueOf(typeFactory.getJavaClass(field.getType())),
                field.getType().getPrecision(),
                field.getType().getScale()
            ));
        }

        return b.build();
    }

    /** */
    private boolean single(SqlNode sqlNode) {
        return !(sqlNode instanceof SqlNodeList);
    }

    /** */
    private void onMessage(UUID nodeId, QueryStartRequest msg) {
        assert nodeId != null && msg != null;

        PlanningContext ctx = createContext(msg.schema(), nodeId, msg.topologyVersion());

        try {
            ExecutionContext<Row> execCtx = new ExecutionContext<>(
                taskExecutor(),
                ctx,
                msg.queryId(),
                msg.fragmentDescription(),
                handler,
                Commons.parametersMap(msg.parameters())
            );

            Node<Row> node = new LogicalRelImplementor<>(
                execCtx,
                partitionService(),
                mailboxRegistry(),
                exchangeService(),
                failureProcessor())
                .go(fromJson(ctx, msg.root()));

            assert node instanceof Outbox : node;

            node.context().execute(((Outbox<Row>) node)::init);

            messageService().send(nodeId, new QueryStartResponse(msg.queryId(), msg.fragmentDescription().fragmentId()));
        }
        catch (Throwable ex) { // TODO don't catch errors!
            cancelQuery(msg.queryId());

            if (ex instanceof ClusterTopologyCheckedException)
                return;

            U.warn(log, "Failed to start query. [nodeId=" + nodeId + ']', ex);

            try {
                messageService().send(nodeId, new QueryStartResponse(msg.queryId(), msg.fragmentDescription().fragmentId(), ex));
            }
            catch (IgniteCheckedException e) {
                e.addSuppressed(ex);

                U.warn(log, "Failed to send reply. [nodeId=" + nodeId + ']', e);
            }

            if (ex instanceof Error)
                throw (Error)ex;
        }
    }

    /** */
    private void onMessage(UUID nodeId, QueryStartResponse msg) {
        assert nodeId != null && msg != null;

        QueryInfo info = running.get(msg.queryId());

        if (info != null)
            info.onResponse(nodeId, msg.fragmentId(), msg.error());
    }

    /** */
    private void onCursorClose(RootNode<?> rootNode) {
        assert rootNode.state() != RootNode.State.RUNNING;

        running.remove(rootNode.queryId());
    }

    /** */
    private void onNodeLeft(UUID nodeId) {
        running.forEach((uuid, queryInfo) -> queryInfo.onNodeLeft(nodeId));

        final Predicate<Node<?>> p = new OriginatingFilter(nodeId);

        mailboxRegistry().outboxes(null).stream()
            .filter(p).forEach(this::executeCancel);

        mailboxRegistry().inboxes(null).stream()
            .filter(p).forEach(this::executeCancel);
    }

    /** */
    private void executeCancel(final Node<?> node) {
        node.context().execute(() -> {
            U.closeQuiet(node);
        });
    }

    /** */
    private enum QueryState {
        /** */
        RUNNING,

        /** */
        CANCELLING,

        /** */
        CANCELLED
    }

    /** */
    private static final class RemoteFragmentKey {
        /** */
        private final UUID nodeId;

        /** */
        private final long fragmentId;

        /** */
        private RemoteFragmentKey(UUID nodeId, long fragmentId) {
            this.nodeId = nodeId;
            this.fragmentId = fragmentId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            RemoteFragmentKey that = (RemoteFragmentKey) o;

            if (fragmentId != that.fragmentId)
                return false;
            return nodeId.equals(that.nodeId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = nodeId.hashCode();
            res = 31 * res + (int) (fragmentId ^ (fragmentId >>> 32));
            return res;
        }
    }

    /** */
    @SuppressWarnings("TypeMayBeWeakened")
    private final class QueryInfo implements QueryCancellable {
        /** */
        private final ExecutionContext<Row> ctx;

        /** */
        private final RootNode<Row> root;

        /** remote nodes */
        private final Set<UUID> remotes;

        /** node to fragment */
        private final Set<RemoteFragmentKey> waiting;

        /** */
        private QueryState state;

        /** */
        private Throwable error;

        /** */
        private QueryInfo(ExecutionContext<Row> ctx, MultiStepPlan plan, Node<Row> root) {
            this.ctx = ctx;

            RootNode<Row> rootNode = new RootNode<>(ctx, ExecutionServiceImpl.this::onCursorClose);
            rootNode.register(root);

            this.root = rootNode;

            remotes = new HashSet<>();
            waiting = new HashSet<>();

            for (int i = 1; i < plan.fragments().size(); i++) {
                Fragment fragment = plan.fragments().get(i);
                List<UUID> nodes = plan.fragmentMapping(fragment).nodes();

                remotes.addAll(nodes);

                for (UUID node : nodes)
                    waiting.add(new RemoteFragmentKey(node, fragment.fragmentId()));
            }

            state = QueryState.RUNNING;
        }

        /** */
        public Iterator<Row> iterator() {
            return iteratorsHolder().iterator(root);
        }

        /** {@inheritDoc} */
        @Override public void doCancel() {
            cancel();
        }

        /** */
        private void awaitAllReplies() {
            Throwable error;

            try {
                synchronized (this) {
                    while (!waiting.isEmpty())
                        wait();

                    error = this.error;
                }
            }
            catch (InterruptedException e) {
                throw new IgniteInterruptedException(e);
            }

            if (error != null)
                throw new IgniteSQLException("Failed to execute query.", error);
        }

        /**
         * Can be called multiple times after receive each error at {@link #onResponse(RemoteFragmentKey, Throwable)}.
         */
        private void cancel() {
            QueryState state0 = null;

            synchronized (this) {
                if (state == QueryState.CANCELLED)
                    return;

                if (state == QueryState.RUNNING)
                    state0 = state = QueryState.CANCELLING;

                if (state == QueryState.CANCELLING && waiting.isEmpty())
                    state0 = state = QueryState.CANCELLED;
            }

            if (state0 == QueryState.CANCELLED) {
                root.close();

                running.remove(ctx.queryId());
            }
        }

        /** */
        private void onNodeLeft(UUID nodeId) {
            List<RemoteFragmentKey> fragments = null;

            synchronized (this) {
                for (RemoteFragmentKey fragment : waiting) {
                    if (!fragment.nodeId.equals(nodeId))
                        continue;

                    if (fragments == null)
                        fragments = new ArrayList<>();

                    fragments.add(fragment);
                }
            }

            if (!F.isEmpty(fragments)) {
                ClusterTopologyCheckedException ex = new ClusterTopologyCheckedException("Failed to start query, node left. nodeId=" + nodeId);

                for (RemoteFragmentKey fragment : fragments)
                    onResponse(fragment, ex);
            }
        }

        /** */
        private void onResponse(UUID nodeId, long fragmentId, Throwable error) {
            onResponse(new RemoteFragmentKey(nodeId, fragmentId), error);
        }

        /** */
        private void onResponse(RemoteFragmentKey fragment, Throwable error) {
            boolean cancel;

            synchronized (this) {
                if (!waiting.remove(fragment))
                    return;

                if (error != null) {
                    if (this.error != null)
                        this.error.addSuppressed(error);
                    else
                        this.error = error;
                }

                boolean empty = waiting.isEmpty();

                cancel = empty && this.error != null;

                if (empty)
                    notifyAll();
            }

            if (cancel)
                cancel();
        }
    }

    /** */
    private static final class OriginatingFilter implements Predicate<Node<?>> {
        /** */
        private final UUID nodeId;

        /** */
        private OriginatingFilter(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public boolean test(Node node) {
            // Uninitialized inbox doesn't know originating node ID.
            return Objects.equals(node.context().originatingNodeId(), nodeId);
        }
    }
}
