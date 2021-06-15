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
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.ValidationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
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
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryCancellable;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.cmd.CommandHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.RootNode;
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
import org.apache.ignite.internal.processors.query.calcite.prepare.CacheKey;
import org.apache.ignite.internal.processors.query.calcite.prepare.CommandPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.ExplainPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.FieldsMetadata;
import org.apache.ignite.internal.processors.query.calcite.prepare.FieldsMetadataImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.FragmentPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepDmlPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepQueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCache;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryTemplate;
import org.apache.ignite.internal.processors.query.calcite.prepare.Splitter;
import org.apache.ignite.internal.processors.query.calcite.prepare.ValidationResult;
import org.apache.ignite.internal.processors.query.calcite.prepare.cmd.SqlToCommandConverter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCommand;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTraitDef;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.ListFieldsQueryCursor;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.processors.query.calcite.exec.PlannerHelper.optimize;
import static org.apache.ignite.internal.processors.query.calcite.externalize.RelJsonReader.fromJson;

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
    private GridCachePartitionExchangeManager<?, ?> exchangeMgr;

    /** */
    private QueryPlanCache qryPlanCache;

    /** */
    private SchemaHolder schemaHolder;

    /** */
    private QueryTaskExecutor taskExecutor;

    /** */
    private FailureProcessor failureProcessor;

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
    private ClosableIteratorsHolder iteratorsHolder;

    /** */
    private final Map<UUID, QueryInfo> running;

    /** */
    private final RowHandler<Row> handler;

    /** */
    private final CommandHandler cmdHnd;

    /** */
    private final SqlToCommandConverter cmdConverter;

    /**
     * @param ctx Kernal.
     */
    public ExecutionServiceImpl(GridKernalContext ctx, RowHandler<Row> handler) {
        super(ctx);
        this.handler = handler;

        discoLsnr = (e, c) -> onNodeLeft(e.eventNode().id());
        running = new ConcurrentHashMap<>();
        cmdConverter = new SqlToCommandConverter();

        cmdHnd = new CommandHandler(
            ctx::query, ctx.cache(), ctx.security(), () -> schemaHolder().schema()
        );
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
        PlanningContext pctx = createContext(Commons.convert(ctx), topologyVersion(), localNodeId(), schema, qry, params);

        List<QueryPlan> qryPlans = queryPlanCache().queryPlan(pctx, new CacheKey(pctx.schemaName(), pctx.query()), this::prepareQuery);

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
        QueryInfo info = running.get(qryId);

        if (info != null)
            info.doCancel();
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
        messageService().register((n, m) -> onMessage(n, (QueryStartRequest) m), MessageType.QUERY_START_REQUEST);
        messageService().register((n, m) -> onMessage(n, (QueryStartResponse) m), MessageType.QUERY_START_RESPONSE);
        messageService().register((n, m) -> onMessage(n, (ErrorMessage) m), MessageType.QUERY_ERROR_MESSAGE);

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
    private PlanningContext createContext(Context parent, AffinityTopologyVersion topVer, UUID originator,
        @Nullable String schema, String qry, Object[] params) {
        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE,
            DistributionTraitDef.INSTANCE,
            RewindabilityTraitDef.INSTANCE,
            CorrelationTraitDef.INSTANCE,
        };

        return PlanningContext.builder()
            .localNodeId(localNodeId())
            .originatingNodeId(originator)
            .parentContext(parent)
            .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema != null
                    ? schemaHolder().schema().getSubSchema(schema)
                    : schemaHolder().schema())
                .traitDefs(traitDefs)
                .build())
            .query(qry)
            .parameters(params)
            .topologyVersion(topVer)
            .logger(log)
            .build();
    }

    /** */
    private List<QueryPlan> prepareQuery(PlanningContext ctx) {
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
    private List<QueryPlan> prepareFragment(PlanningContext ctx) {
        return ImmutableList.of(new FragmentPlan(fromJson(ctx, ctx.query())));
    }

    /** */
    private QueryPlan prepareSingle(SqlNode sqlNode, PlanningContext ctx) throws ValidationException {
        assert single(sqlNode);

        ctx.planner().reset();

        if (sqlNode instanceof IgniteSqlCommand)
            return prepareCommand(sqlNode, ctx);

        switch (sqlNode.getKind()) {
            case SELECT:
            case ORDER_BY:
            case WITH:
            case VALUES:
            case UNION:
            case EXCEPT:
            case INTERSECT:
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

        IgniteRel igniteRel = optimize(sqlNode, planner, log);

        // Split query plan to query fragments.
        List<Fragment> fragments = new Splitter().go(igniteRel);

        QueryTemplate template = new QueryTemplate(mappingSvc, fragments);

        return new MultiStepQueryPlan(template, queryFieldsMetadata(ctx, validated.dataType(), validated.origins()));
    }

    /** */
    private QueryPlan prepareDml(SqlNode sqlNode, PlanningContext ctx) throws ValidationException {
        IgnitePlanner planner = ctx.planner();

        // Validate
        sqlNode = planner.validate(sqlNode);

        // Convert to Relational operators graph
        IgniteRel igniteRel = optimize(sqlNode, planner, log);

        // Split query plan to query fragments.
        List<Fragment> fragments = new Splitter().go(igniteRel);

        QueryTemplate template = new QueryTemplate(mappingSvc, fragments);

        return new MultiStepDmlPlan(template, queryFieldsMetadata(ctx, igniteRel.getRowType(), null));
    }

    /** */
    private QueryPlan prepareCommand(SqlNode sqlNode, PlanningContext ctx) {
        assert sqlNode instanceof IgniteSqlCommand : sqlNode == null ? "null" : sqlNode.getClass().getName();

        return new CommandPlan(cmdConverter.convert((IgniteSqlCommand)sqlNode, ctx));
    }

    /** */
    private QueryPlan prepareExplain(SqlNode explain, PlanningContext ctx) throws ValidationException {
        IgnitePlanner planner = ctx.planner();

        SqlNode sql = ((SqlExplain)explain).getExplicandum();

        // Validate
        sql = planner.validate(sql);

        // Convert to Relational operators graph
        IgniteRel igniteRel = optimize(sql, planner, log);

        String plan = RelOptUtil.toString(igniteRel, SqlExplainLevel.ALL_ATTRIBUTES);

        return new ExplainPlan(plan, explainFieldsMetadata(ctx));
    }

    /** */
    private FieldsMetadata explainFieldsMetadata(PlanningContext ctx) {
        IgniteTypeFactory factory = ctx.typeFactory();
        RelDataType planStrDataType =
            factory.createSqlType(SqlTypeName.VARCHAR, PRECISION_NOT_SPECIFIED);
        T2<String, RelDataType> planField = new T2<>(ExplainPlan.PLAN_COL_NAME, planStrDataType);
        RelDataType planDataType = factory.createStructType(singletonList(planField));

        return queryFieldsMetadata(ctx, planDataType, null);
    }

    /** */
    private FieldsQueryCursor<List<?>> executePlan(UUID qryId, PlanningContext pctx, QueryPlan plan) {
        switch (plan.type()) {
            case DML:
                // TODO a barrier between previous operation and this one
            case QUERY:
                return executeQuery(qryId, (MultiStepPlan) plan, pctx);
            case EXPLAIN:
                return executeExplain((ExplainPlan)plan, pctx);
            case COMMAND:
                return executeCommand(qryId, (CommandPlan)plan, pctx);
            default:
                throw new AssertionError("Unexpected plan type: " + plan);
        }
    }

    /** */
    private FieldsQueryCursor<List<?>> executeCommand(UUID qryId, CommandPlan plan, PlanningContext pctx) {
        try {
            cmdHnd.handle(qryId, plan.command(), pctx);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSQLException("Failed to execute command [stmt=" + pctx.query() +
                ", err=" + e.getMessage() + ']', e);
        }

        return H2Utils.zeroCursor();
    }

    /** */
    private FieldsQueryCursor<List<?>> executeQuery(UUID qryId, MultiStepPlan plan, PlanningContext pctx) {
        plan.init(pctx);

        List<Fragment> fragments = plan.fragments();

        // Local execution
        Fragment fragment = F.first(fragments);

        if (U.assertionsEnabled()) {
            assert fragment != null;

            FragmentMapping mapping = plan.mapping(fragment);

            assert mapping != null;

            List<UUID> nodes = mapping.nodeIds();

            assert nodes != null && nodes.size() == 1 && F.first(nodes).equals(pctx.localNodeId());
        }

        FragmentDescription fragmentDesc = new FragmentDescription(
            fragment.fragmentId(),
            plan.mapping(fragment),
            plan.target(fragment),
            plan.remotes(fragment));

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
        for (int i = 1; i < fragments.size(); i++) {
            fragment = fragments.get(i);
            fragmentDesc = new FragmentDescription(
                fragment.fragmentId(),
                plan.mapping(fragment),
                plan.target(fragment),
                plan.remotes(fragment));

            Throwable ex = null;
            for (UUID nodeId : fragmentDesc.nodeIds()) {
                if (ex != null)
                    info.onResponse(nodeId, fragment.fragmentId(), ex);
                else {
                    try {
                        QueryStartRequest req = new QueryStartRequest(
                            qryId,
                            pctx.schemaName(),
                            fragment.serialized(),
                            pctx.topologyVersion(),
                            fragmentDesc,
                            pctx.parameters());

                        messageService().send(nodeId, req);
                    }
                    catch (Throwable e) {
                        info.onResponse(nodeId, fragment.fragmentId(), ex = e);
                    }
                }
            }
        }

        return new ListFieldsQueryCursor<>(plan, info.iterator(), ectx);
    }

    /** */
    private FieldsQueryCursor<List<?>> executeExplain(ExplainPlan plan, PlanningContext pctx) {
        QueryCursorImpl<List<?>> cur = new QueryCursorImpl<>(singletonList(singletonList(plan.plan())));
        cur.fieldsMeta(plan.fieldsMeta().queryFieldsMetadata(pctx.typeFactory()));

        return cur;
    }

    /** */
    private void executeFragment(UUID qryId, FragmentPlan plan, PlanningContext pctx, FragmentDescription fragmentDesc) {
        ExecutionContext<Row> ectx = new ExecutionContext<>(taskExecutor(), pctx, qryId,
            fragmentDesc, handler, Commons.parametersMap(pctx.parameters()));

        long frId = fragmentDesc.fragmentId();
        UUID origNodeId = pctx.originatingNodeId();

        Outbox<Row> node = new LogicalRelImplementor<>(
                ectx,
                partitionService(),
                mailboxRegistry(),
                exchangeService(),
                failureProcessor())
                .go(plan.root());

        try {
            messageService().send(origNodeId, new QueryStartResponse(qryId, frId));
        }
        catch (IgniteCheckedException e) {
            IgniteException wrpEx = new IgniteException("Failed to send reply. [nodeId=" + origNodeId + ']', e);

            throw wrpEx;
        }

        node.init();
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
    private FieldsMetadata queryFieldsMetadata(PlanningContext ctx, RelDataType sqlType,
        @Nullable List<List<String>> origins) {
        RelDataType resultType = TypeUtils.getResultType(
            ctx.typeFactory(), ctx.catalogReader(), sqlType, origins);
        return new FieldsMetadataImpl(resultType, origins);
    }

    /** */
    private boolean single(SqlNode sqlNode) {
        return !(sqlNode instanceof SqlNodeList);
    }

    /** */
    private void onMessage(UUID nodeId, QueryStartRequest msg) {
        assert nodeId != null && msg != null;

        try {
            PlanningContext pctx = createContext(Contexts.empty(), msg.topologyVersion(), nodeId, msg.schema(),
                msg.root(), msg.parameters());

            List<QueryPlan> qryPlans = queryPlanCache().queryPlan(
                pctx,
                new CacheKey(pctx.schemaName(), pctx.query()),
                this::prepareFragment
            );

            assert qryPlans.size() == 1 && qryPlans.get(0).type() == QueryPlan.Type.FRAGMENT;

            FragmentPlan plan = (FragmentPlan)qryPlans.get(0);

            executeFragment(msg.queryId(), plan, pctx, msg.fragmentDescription());
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

                throw wrpEx;
            }

            throw ex;
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
    private void onMessage(UUID nodeId, ErrorMessage msg) {
        assert nodeId != null && msg != null;

        QueryInfo info = running.get(msg.queryId());

        if (info != null)
            info.onError(new RemoteException(nodeId, msg.queryId(), msg.fragmentId(), msg.error()));
    }

    /** */
    private void onNodeLeft(UUID nodeId) {
        running.forEach((uuid, queryInfo) -> queryInfo.onNodeLeft(nodeId));
    }

    /** */
    private enum QueryState {
        /** */
        RUNNING,

        /** */
        CLOSING,

        /** */
        CLOSED
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
        private volatile QueryState state;

        /** */
        private QueryInfo(ExecutionContext<Row> ctx, MultiStepPlan plan, Node<Row> root) {
            this.ctx = ctx;

            RootNode<Row> rootNode = new RootNode<>(ctx, plan.fieldsMetadata().rowType(), this::tryClose);
            rootNode.register(root);

            this.root = rootNode;

            remotes = new HashSet<>();
            waiting = new HashSet<>();

            for (int i = 1; i < plan.fragments().size(); i++) {
                Fragment fragment = plan.fragments().get(i);
                List<UUID> nodes = plan.mapping(fragment).nodeIds();

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
            root.close();
        }

        /**
         * Can be called multiple times after receive each error at {@link #onResponse(RemoteFragmentKey, Throwable)}.
         */
        private void tryClose() {
            QueryState state0 = null;

            synchronized (this) {
                if (state == QueryState.CLOSED)
                    return;

                if (state == QueryState.RUNNING)
                    state0 = state = QueryState.CLOSING;

                // 1) close local fragment
                root.closeInternal();

                if (state == QueryState.CLOSING && waiting.isEmpty())
                    state0 = state = QueryState.CLOSED;
            }

            if (state0 == QueryState.CLOSED) {
                // 2) unregister runing query
                running.remove(ctx.queryId());

                // 4) close remote fragments
                IgniteException wrpEx = null;

                // 3) close remote fragments
                for (UUID nodeId : remotes) {
                    try {
                        exchangeService().closeOutbox(nodeId, ctx.queryId(), -1, -1);
                    }
                    catch (IgniteCheckedException e) {
                        if (wrpEx == null)
                            wrpEx = new IgniteException("Failed to send cancel message. [nodeId=" + nodeId + ']', e);
                        else
                            wrpEx.addSuppressed(e);
                    }
                }

                // 4) Cancel local fragment
                ctx.cancel();

                if (wrpEx != null)
                    throw wrpEx;
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
                ClusterTopologyCheckedException ex = new ClusterTopologyCheckedException(
                    "Failed to start query, node left. nodeId=" + nodeId);

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
            QueryState state;
            synchronized (this) {
                waiting.remove(fragment);
                state = this.state;
            }

            if (error != null)
                onError(error);
            else if (state == QueryState.CLOSING)
                tryClose();
        }

        /** */
        private void onError(Throwable error) {
            root.onError(error);

            tryClose();
        }
    }
}
