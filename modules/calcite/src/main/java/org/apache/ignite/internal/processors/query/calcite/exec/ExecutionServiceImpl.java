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

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.ValidationException;
import org.apache.ignite.internal.processors.query.calcite.ResultSetMetadata;
import org.apache.ignite.internal.processors.query.calcite.SqlCursor;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.RootNode;
import org.apache.ignite.internal.processors.query.calcite.message.ErrorMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartRequest;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartResponse;
import org.apache.ignite.internal.processors.query.calcite.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.processors.query.calcite.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.RemoteException;
import org.apache.ignite.internal.processors.query.calcite.prepare.CacheKey;
import org.apache.ignite.internal.processors.query.calcite.prepare.DdlPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.ExplainPlan;
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
import org.apache.ignite.internal.processors.query.calcite.prepare.ResultSetMetadataImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.ResultSetMetadataInternal;
import org.apache.ignite.internal.processors.query.calcite.prepare.Splitter;
import org.apache.ignite.internal.processors.query.calcite.prepare.ValidationResult;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTraitDef;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.NodeLeaveHandler;
import org.apache.ignite.internal.processors.query.calcite.util.TransformingIterator;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.Cancellable;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.ignite.internal.processors.query.calcite.exec.PlannerHelper.optimize;
import static org.apache.ignite.internal.processors.query.calcite.externalize.RelJsonReader.fromJson;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

/**
 *
 */
public class ExecutionServiceImpl<Row> implements ExecutionService {
    private static final IgniteLogger LOG = IgniteLogger.forClass(ExecutionServiceImpl.class);

    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    /** */
    private final TopologyService topSrvc;

    /** */
    private final MessageService msgSrvc;

    /** */
    private final String locNodeId;

    /** */
    private final QueryPlanCache qryPlanCache;

    /** */
    private final SchemaHolder schemaHolder;

    /** */
    private final QueryTaskExecutor taskExecutor;

    /** */
    private final AffinityService affSrvc;

    /** */
    private final MailboxRegistry mailboxRegistry;

    /** */
    private final MappingService mappingSrvc;

    /** */
    private final ExchangeService exchangeSrvc;

    /** */
    private final ClosableIteratorsHolder iteratorsHolder;

    /** */
    private final Map<UUID, QueryInfo> running;

    /** */
    private final RowHandler<Row> handler;

    /** */
    private final DdlSqlToCommandConverter ddlConverter;

    public ExecutionServiceImpl(
        TopologyService topSrvc,
        MessageService msgSrvc,
        QueryPlanCache planCache,
        SchemaHolder schemaHolder,
        QueryTaskExecutor taskExecutor,
        RowHandler<Row> handler
    ) {
        this.topSrvc = topSrvc;
        this.handler = handler;
        this.msgSrvc = msgSrvc;
        this.schemaHolder = schemaHolder;
        this.taskExecutor = taskExecutor;

        locNodeId = topSrvc.localMember().id();
        qryPlanCache = planCache;
        running = new ConcurrentHashMap<>();
        ddlConverter = new DdlSqlToCommandConverter();
        iteratorsHolder = new ClosableIteratorsHolder(topSrvc.localMember().name(), LOG);
        mailboxRegistry = new MailboxRegistryImpl(topSrvc);
        exchangeSrvc = new ExchangeServiceImpl(taskExecutor, mailboxRegistry, msgSrvc);
        mappingSrvc = new MappingServiceImpl(topSrvc);
        // TODO: fix this
        affSrvc = cacheId -> Objects::hashCode;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        iteratorsHolder.start();
        mailboxRegistry.start();
        exchangeSrvc.start();

        topSrvc.addEventHandler(new NodeLeaveHandler(this::onNodeLeft));

        msgSrvc.register((n, m) -> onMessage(n, (QueryStartRequest) m), SqlQueryMessageGroup.QUERY_START_REQUEST);
        msgSrvc.register((n, m) -> onMessage(n, (QueryStartResponse) m), SqlQueryMessageGroup.QUERY_START_RESPONSE);
        msgSrvc.register((n, m) -> onMessage(n, (ErrorMessage) m), SqlQueryMessageGroup.ERROR_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public List<SqlCursor<List<?>>> executeQuery(
        String schema,
        String qry,
        Object[] params
    ) {
        PlanningContext pctx = createContext(topologyVersion(), locNodeId, schema, qry, params);

        List<QueryPlan> qryPlans = qryPlanCache.queryPlan(pctx, new CacheKey(pctx.schemaName(), pctx.query()), this::prepareQuery);

        return executePlans(qryPlans, pctx);
    }

    /**
     * Executes prepared plans.
     * @param qryPlans Query plans.
     * @param pctx Query context.
     * @return List of query result cursors.
     */
    @NotNull public List<SqlCursor<List<?>>> executePlans(
        Collection<QueryPlan> qryPlans,
        PlanningContext pctx
    ) {
        List<SqlCursor<List<?>>> cursors = new ArrayList<>(qryPlans.size());

        for (QueryPlan plan : qryPlans) {
            UUID qryId = UUID.randomUUID();

            SqlCursor<List<?>> cur = executePlan(qryId, pctx, plan);

            cursors.add(cur);
        }

        return cursors;
    }

    /** {@inheritDoc} */
    @Override public void cancelQuery(UUID qryId) {
        QueryInfo info = running.get(qryId);

        if (info != null)
            info.cancel();
    }

    /** */
    protected long topologyVersion() {
        return 1L;
    }

    /** */
    private PlanningContext createContext(long topVer, String originator,
        @Nullable String schema, String qry, Object[] params) {
        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE,
            DistributionTraitDef.INSTANCE,
            RewindabilityTraitDef.INSTANCE,
            CorrelationTraitDef.INSTANCE,
        };

        return PlanningContext.builder()
            .localNodeId(locNodeId)
            .originatingNodeId(originator)
            .parentContext(Contexts.empty())
            .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema != null
                    ? schemaHolder.schema().getSubSchema(schema)
                    : schemaHolder.schema())
                .traitDefs(traitDefs)
                .build())
            .query(qry)
            .parameters(params)
            .topologyVersion(topVer)
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
        catch (SqlParseException e) {
            throw new IgniteInternalException("Failed to parse query", e);
        }
        catch (ValidationException e) {
            throw new IgniteInternalException("Failed to validate query", e);
        }
        catch (Exception e) {
            throw new IgniteInternalException("Failed to plan query.", e);
        }
    }

    /** */
    private List<QueryPlan> prepareFragment(PlanningContext ctx) {
        return List.of(new FragmentPlan(fromJson(ctx, ctx.query())));
    }

    /** */
    private QueryPlan prepareSingle(SqlNode sqlNode, PlanningContext ctx) throws ValidationException {
        assert single(sqlNode);

        ctx.planner().reset();

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

            case CREATE_TABLE:
            case DROP_TABLE:
                return prepareDdl(sqlNode, ctx);

            default:
                throw new IgniteInternalException("Unsupported operation [" +
                    "sqlNodeKind=" + sqlNode.getKind() + "; " +
                    "querySql=\"" + ctx.query() + "\"]");
        }
    }

    /** */
    private QueryPlan prepareQuery(SqlNode sqlNode, PlanningContext ctx) {
        IgnitePlanner planner = ctx.planner();

        // Validate
        ValidationResult validated = planner.validateAndGetTypeMetadata(sqlNode);

        sqlNode = validated.sqlNode();

        IgniteRel igniteRel = optimize(sqlNode, planner, LOG);

        // Split query plan to query fragments.
        List<Fragment> fragments = new Splitter().go(igniteRel);

        QueryTemplate template = new QueryTemplate(mappingSrvc, fragments);

        return new MultiStepQueryPlan(template, resultSetMetadata(ctx, validated.dataType(), validated.origins()));
    }

    /** */
    private QueryPlan prepareDml(SqlNode sqlNode, PlanningContext ctx) throws ValidationException {
        IgnitePlanner planner = ctx.planner();

        // Validate
        sqlNode = planner.validate(sqlNode);

        // Convert to Relational operators graph
        IgniteRel igniteRel = optimize(sqlNode, planner, LOG);

        // Split query plan to query fragments.
        List<Fragment> fragments = new Splitter().go(igniteRel);

        QueryTemplate template = new QueryTemplate(mappingSrvc, fragments);

        return new MultiStepDmlPlan(template, resultSetMetadata(ctx, igniteRel.getRowType(), null));
    }

    /** */
    private QueryPlan prepareDdl(SqlNode sqlNode, PlanningContext ctx) {
        assert sqlNode instanceof SqlDdl : sqlNode == null ? "null" : sqlNode.getClass().getName();

        SqlDdl ddlNode = (SqlDdl)sqlNode;

        return new DdlPlan(ddlConverter.convert(ddlNode, ctx));
    }

    /** */
    private QueryPlan prepareExplain(SqlNode explain, PlanningContext ctx) throws ValidationException {
        IgnitePlanner planner = ctx.planner();

        SqlNode sql = ((SqlExplain)explain).getExplicandum();

        // Validate
        sql = planner.validate(sql);

        // Convert to Relational operators graph
        IgniteRel igniteRel = optimize(sql, planner, LOG);

        String plan = RelOptUtil.toString(igniteRel, SqlExplainLevel.ALL_ATTRIBUTES);

        return new ExplainPlan(plan, explainFieldsMetadata(ctx));
    }

    /** */
    private ResultSetMetadata explainFieldsMetadata(PlanningContext ctx) {
        IgniteTypeFactory factory = ctx.typeFactory();
        RelDataType planStrDataType =
            factory.createSqlType(SqlTypeName.VARCHAR, PRECISION_NOT_SPECIFIED);
        Map.Entry<String, RelDataType> planField = new IgniteBiTuple<>(ExplainPlan.PLAN_COL_NAME, planStrDataType);
        RelDataType planDataType = factory.createStructType(singletonList(planField));

        return resultSetMetadata(ctx, planDataType, null);
    }

    /** */
    private SqlCursor<List<?>> executePlan(UUID qryId, PlanningContext pctx, QueryPlan plan) {
        switch (plan.type()) {
            case DML:
                // TODO a barrier between previous operation and this one
            case QUERY:
                return executeQuery(qryId, (MultiStepPlan) plan, pctx);
            case EXPLAIN:
                return executeExplain((ExplainPlan)plan);
            case DDL:
                return executeDdl((DdlPlan)plan, pctx);

            default:
                throw new AssertionError("Unexpected plan type: " + plan);
        }
    }

    /** */
    private SqlCursor<List<?>> executeDdl(DdlPlan plan, PlanningContext pctx) {
        throw new UnsupportedOperationException("plan=" + plan + ", ctx=" + pctx);
    }

    /** */
    private SqlCursor<List<?>> executeQuery(UUID qryId, MultiStepPlan plan, PlanningContext pctx) {
        plan.init(pctx);

        List<Fragment> fragments = plan.fragments();

        // Local execution
        Fragment fragment = first(fragments);

        if (IgniteUtils.assertionsEnabled()) {
            assert fragment != null;

            FragmentMapping mapping = plan.mapping(fragment);

            assert mapping != null;

            List<String> nodes = mapping.nodeIds();

            assert nodes != null && nodes.size() == 1 && first(nodes).equals(pctx.localNodeId());
        }

        FragmentDescription fragmentDesc = new FragmentDescription(
            fragment.fragmentId(),
            plan.mapping(fragment),
            plan.target(fragment),
            plan.remotes(fragment));

        ExecutionContext<Row> ectx = new ExecutionContext<>(
            taskExecutor,
            pctx,
            qryId,
            fragmentDesc,
            handler,
            Commons.parametersMap(pctx.parameters()));

        Node<Row> node = new LogicalRelImplementor<>(ectx, affSrvc, mailboxRegistry,
            exchangeSrvc).go(fragment.root());

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
            for (String nodeId : fragmentDesc.nodeIds()) {
                if (ex != null)
                    info.onResponse(nodeId, fragment.fragmentId(), ex);
                else {
                    try {
                        QueryStartRequest req = FACTORY.queryStartRequest()
                            .queryId(qryId)
                            .fragmentId(fragment.fragmentId())
                            .schema(pctx.schemaName())
                            .root(fragment.serialized())
                            .topologyVersion(pctx.topologyVersion())
                            .fragmentDescription(fragmentDesc)
                            .parameters(pctx.parameters())
                            .build();

                        msgSrvc.send(nodeId, req);
                    }
                    catch (Throwable e) {
                        info.onResponse(nodeId, fragment.fragmentId(), ex = e);
                    }
                }
            }
        }

        return Commons.createCursor(new TransformingIterator<>(info.iterator(), row -> {
            int rowSize = ectx.rowHandler().columnCount(row);

            List<Object> res = new ArrayList<>(rowSize);

            for (int i = 0; i < rowSize; i++)
                res.add(ectx.rowHandler().get(i, row));

            return res;
        }), plan);
    }

    /** */
    private SqlCursor<List<?>> executeExplain(ExplainPlan plan) {
        SqlCursor<List<?>> cur = Commons.createCursor(singletonList(singletonList(plan.plan())), plan);
        // TODO: fix this
//        cur.fieldsMeta(plan.fieldsMeta().queryFieldsMetadata(pctx.typeFactory()));

        return cur;
    }

    /** */
    private void executeFragment(UUID qryId, FragmentPlan plan, PlanningContext pctx, FragmentDescription fragmentDesc) {
        ExecutionContext<Row> ectx = new ExecutionContext<>(taskExecutor, pctx, qryId,
            fragmentDesc, handler, Commons.parametersMap(pctx.parameters()));

        long frId = fragmentDesc.fragmentId();
        String origNodeId = pctx.originatingNodeId();

        Outbox<Row> node = new LogicalRelImplementor<>(
            ectx,
            affSrvc,
            mailboxRegistry,
            exchangeSrvc
        ).go(plan.root());

        try {
            msgSrvc.send(
                origNodeId,
                FACTORY.queryStartResponse()
                    .queryId(qryId)
                    .fragmentId(frId)
                    .build()
            );
        }
        catch (IgniteInternalCheckedException e) {
            IgniteInternalException wrpEx = new IgniteInternalException("Failed to send reply. [nodeId=" + origNodeId + ']', e);

            throw wrpEx;
        }

        node.init();
    }

    /** */
    private void register(QueryInfo info) {
        UUID qryId = info.ctx.queryId();

        running.put(qryId, info);
    }

    /** */
    private ResultSetMetadataInternal resultSetMetadata(PlanningContext ctx, RelDataType sqlType,
        @Nullable List<List<String>> origins) {
        return new ResultSetMetadataImpl(
            TypeUtils.getResultType(ctx.typeFactory(), ctx.catalogReader(), sqlType, origins),
            origins
        );
    }

    /** */
    private boolean single(SqlNode sqlNode) {
        return !(sqlNode instanceof SqlNodeList);
    }

    /** */
    private void onMessage(String nodeId, QueryStartRequest msg) {
        assert nodeId != null && msg != null;

        try {
            PlanningContext pctx = createContext(msg.topologyVersion(), nodeId, msg.schema(),
                msg.root(), msg.parameters());

            List<QueryPlan> qryPlans = qryPlanCache.queryPlan(
                pctx,
                new CacheKey(pctx.schemaName(), pctx.query()),
                this::prepareFragment
            );

            assert qryPlans.size() == 1 && qryPlans.get(0).type() == QueryPlan.Type.FRAGMENT;

            FragmentPlan plan = (FragmentPlan)qryPlans.get(0);

            executeFragment(msg.queryId(), plan, pctx, msg.fragmentDescription());
        }
        catch (Throwable ex) {
            LOG.error("Failed to start query fragment", ex);

            mailboxRegistry.outboxes(msg.queryId(), msg.fragmentId(), -1)
                .forEach(Outbox::close);
            mailboxRegistry.inboxes(msg.queryId(), msg.fragmentId(), -1)
                .forEach(Inbox::close);

            try {
                msgSrvc.send(
                    nodeId,
                    FACTORY.queryStartResponse()
                        .queryId(msg.queryId())
                        .fragmentId(msg.fragmentId())
                        .error(ex)
                        .build()
                );
            }
            catch (Exception e) {
                LOG.error("Error occurred during send error message", e);

                IgniteInternalException wrpEx = new IgniteInternalException("Error occurred during send error message", e);

                e.addSuppressed(ex);

                throw wrpEx;
            }

            throw ex;
        }
    }

    /** */
    private void onMessage(String nodeId, QueryStartResponse msg) {
        assert nodeId != null && msg != null;

        QueryInfo info = running.get(msg.queryId());

        if (info != null)
            info.onResponse(nodeId, msg.fragmentId(), msg.error());
    }

    /** */
    private void onMessage(String nodeId, ErrorMessage msg) {
        assert nodeId != null && msg != null;

        QueryInfo info = running.get(msg.queryId());

        if (info != null)
            info.onError(new RemoteException(nodeId, msg.queryId(), msg.fragmentId(), msg.error()));
    }

    /** */
    private void onNodeLeft(ClusterNode node) {
        running.forEach((uuid, queryInfo) -> queryInfo.onNodeLeft(node.id()));
    }


    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        IgniteUtils.closeAll(qryPlanCache::stop, iteratorsHolder::stop, mailboxRegistry::stop, exchangeSrvc::stop);
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
        private final String nodeId;

        /** */
        private final long fragmentId;

        /** */
        private RemoteFragmentKey(String nodeId, long fragmentId) {
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
    private final class QueryInfo implements Cancellable {
        /** */
        private final ExecutionContext<Row> ctx;

        /** */
        private final RootNode<Row> root;

        /** remote nodes */
        private final Set<String> remotes;

        /** node to fragment */
        private final Set<RemoteFragmentKey> waiting;

        /** */
        private volatile QueryState state;

        /** */
        private QueryInfo(ExecutionContext<Row> ctx, MultiStepPlan plan, Node<Row> root) {
            this.ctx = ctx;

            RootNode<Row> rootNode = new RootNode<>(ctx, plan.metadata().rowType(), this::tryClose);
            rootNode.register(root);

            this.root = rootNode;

            remotes = new HashSet<>();
            waiting = new HashSet<>();

            for (int i = 1; i < plan.fragments().size(); i++) {
                Fragment fragment = plan.fragments().get(i);
                List<String> nodes = plan.mapping(fragment).nodeIds();

                remotes.addAll(nodes);

                for (String node : nodes)
                    waiting.add(new RemoteFragmentKey(node, fragment.fragmentId()));
            }

            state = QueryState.RUNNING;
        }

        /** */
        public Iterator<Row> iterator() {
            return iteratorsHolder.iterator(root);
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
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
                // 2) unregister running query
                running.remove(ctx.queryId());

                IgniteInternalException wrpEx = null;

                // 3) close remote fragments
                for (String nodeId : remotes) {
                    try {
                        exchangeSrvc.closeOutbox(nodeId, ctx.queryId(), -1, -1);
                    }
                    catch (IgniteInternalCheckedException e) {
                        if (wrpEx == null)
                            wrpEx = new IgniteInternalException("Failed to send cancel message. [nodeId=" + nodeId + ']', e);
                        else
                            wrpEx.addSuppressed(e);
                    }
                }

                // 4) Cancel local fragment
                root.context().execute(ctx::cancel, root::onError);

                if (wrpEx != null)
                    throw wrpEx;
            }
        }

        /** */
        private void onNodeLeft(String nodeId) {
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

            if (!nullOrEmpty(fragments)) {
                IgniteInternalCheckedException ex = new IgniteInternalCheckedException(
                    "Failed to start query, node left. nodeId=" + nodeId);

                for (RemoteFragmentKey fragment : fragments)
                    onResponse(fragment, ex);
            }
        }

        /** */
        private void onResponse(String nodeId, long fragmentId, Throwable error) {
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
