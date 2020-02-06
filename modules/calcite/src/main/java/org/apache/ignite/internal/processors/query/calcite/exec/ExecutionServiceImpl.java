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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
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
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryCancellable;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.message.QueryCancelRequest;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartRequest;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartResponse;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.PartitionService;
import org.apache.ignite.internal.processors.query.calcite.prepare.CacheKey;
import org.apache.ignite.internal.processors.query.calcite.prepare.CalciteQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepPlanImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCache;
import org.apache.ignite.internal.processors.query.calcite.prepare.Splitter;
import org.apache.ignite.internal.processors.query.calcite.prepare.ValidationResult;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.RelOp;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelGraph;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelToGraphConverter;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.SenderNode;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.ListFieldsQueryCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;

/**
 *
 */
public class ExecutionServiceImpl extends AbstractService implements ExecutionService {
    /** */
    private final DiscoveryEventListener discoLsnr;

    /** */
    private UUID localNodeId;

    /** */
    private GridEventStorageManager eventManager;

    /** */
    private GridCachePartitionExchangeManager<?,?> exchangeManager;

    /** */
    private QueryPlanCache queryPlanCache;

    /** */
    private SchemaHolder schemaHolder;

    /** */
    private QueryTaskExecutor taskExecutor;

    /** */
    private FailureProcessor failureProcessor;

    /** */
    private PartitionService partitionService;

    /** */
    private MailboxRegistry mailboxRegistry;

    /** */
    private MappingService mappingService;

    /** */
    private MessageService messageService;

    /** */
    private ExchangeService exchangeService;

    /** */
    private ClosableIteratorsHolder iteratorsHolder;

    /** */
    private final Map<UUID, QueryInfo> running;

    /**
     * @param ctx Kernal.
     */
    public ExecutionServiceImpl(GridKernalContext ctx) {
        super(ctx);

        discoLsnr = (e, c) -> onNodeLeft(e.eventNode().id());
        running = new ConcurrentHashMap<>();
    }

    /**
     * @param localNodeId Local node ID.
     */
    public void localNodeId(UUID localNodeId) {
        this.localNodeId = localNodeId;
    }

    /**
     * @return Local node ID.
     */
    public UUID localNodeId() {
        return localNodeId;
    }

    /**
     * @param queryPlanCache Query cache.
     */
    public void queryPlanCache(QueryPlanCache queryPlanCache) {
        this.queryPlanCache = queryPlanCache;
    }

    /**
     * @return Query cache.
     */
    public QueryPlanCache queryPlanCache() {
        return queryPlanCache;
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
     * @param partitionService Partition service.
     */
    public void partitionService(PartitionService partitionService) {
        this.partitionService = partitionService;
    }

    /**
     * @return Partition service.
     */
    public PartitionService partitionService() {
        return partitionService;
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
     * @param mappingService Mapping service.
     */
    public void mappingService(MappingService mappingService) {
        this.mappingService = mappingService;
    }

    /**
     * @return Mapping service.
     */
    public MappingService mappingService() {
        return mappingService;
    }

    /**
     * @param messageService Message service.
     */
    public void messageService(MessageService messageService) {
        this.messageService = messageService;
    }

    /**
     * @return Message service.
     */
    public MessageService messageService() {
        return messageService;
    }

    /**
     * @param exchangeService Exchange service.
     */
    public void exchangeService(ExchangeService exchangeService) {
        this.exchangeService = exchangeService;
    }

    /**
     * @return Exchange service.
     */
    public ExchangeService exchangeService() {
        return exchangeService;
    }

    /**
     * @param eventManager Event manager.
     */
    public void eventManager(GridEventStorageManager eventManager) {
        this.eventManager = eventManager;
    }

    /**
     * @return Event manager.
     */
    public GridEventStorageManager eventManager() {
        return eventManager;
    }

    /**
     * @param exchangeManager Exchange manager.
     */
    public void exchangeManager(GridCachePartitionExchangeManager<?,?> exchangeManager) {
        this.exchangeManager = exchangeManager;
    }

    /**
     * @return Exchange manager.
     */
    public GridCachePartitionExchangeManager<?, ?> exchangeManager() {
        return exchangeManager;
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
    @Override public List<FieldsQueryCursor<List<?>>> executeQuery(@Nullable QueryContext ctx, String schema, String query, Object[] params) {
        PlanningContext pctx = createContext(ctx, schema, query, params);
        RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(IgniteMetadata.METADATA_PROVIDER));
        try (IgnitePlanner ignored = pctx.planner()) {
            return Commons.transform(prepare(pctx), p -> executeSingle(UUID.randomUUID(), pctx, p));
        }
        finally {
            RelMetadataQuery.THREAD_PROVIDERS.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public void cancelQuery(UUID queryId) {
        mailboxRegistry().outboxes(queryId).forEach(this::executeCancel);
        mailboxRegistry().inboxes(queryId).forEach(this::executeCancel);

        QueryInfo info = running.get(queryId);

        if (info != null)
            info.cancel();
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        localNodeId(ctx.localNodeId());
        exchangeManager(ctx.cache().context().exchange());
        eventManager(ctx.event());
        iteratorsHolder(new ClosableIteratorsHolder(log));

        CalciteQueryProcessor proc = Objects.requireNonNull(Commons.lookupComponent(ctx, CalciteQueryProcessor.class));

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
        messageService().register((n,m) -> onMessage(n, (QueryCancelRequest) m), MessageType.QUERY_CANCEL_REQUEST);

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
    private PlanningContext createContext(@Nullable QueryContext qryCtx, @Nullable String schemaName, String query, Object[] params) {
        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE
            , DistributionTraitDef.INSTANCE
            //, RelCollationTraitDef.INSTANCE TODO
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
            .query(query)
            .parameters(params)
            .topologyVersion(topologyVersion())
            .cancelGroup(cancelGroup(qryCtx))
            .logger(log)
            .build();
    }

    /** */
    private PlanningContext createContext(@Nullable String schemaName, UUID originatingNodeId, AffinityTopologyVersion topVer) {
        // TODO pass to context user locale and timezone.

        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE
            //, RelCollationTraitDef.INSTANCE TODO
        };

        return PlanningContext.builder()
            .localNodeId(localNodeId())
            .originatingNodeId(originatingNodeId)
            .parentContext(Contexts.empty())
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
    private List<QueryPlan> prepare(PlanningContext ctx) {
        return queryPlanCache().queryPlan(ctx, new CacheKey(ctx.schemaName(), ctx.query()), this::prepare0);
    }

    /** */
    private List<QueryPlan> prepare0(PlanningContext ctx) {
        try {
            String query = ctx.query();

            assert query != null;

            // Parse query.
            SqlNode sqlNode = ctx.planner().parse(query);

            if (single(sqlNode))
                return Collections.singletonList(prepareSingle(sqlNode, ctx));

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

        if (SqlKind.QUERY.contains(sqlNode.getKind()))
            return prepareQuery(sqlNode, ctx);

        throw new IgniteSQLException("Unsupported operation [querySql=\"" + ctx.query() + "\"]", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /** */
    private QueryPlan prepareQuery(SqlNode sqlNode, PlanningContext ctx) throws ValidationException {
        IgnitePlanner planner = ctx.planner();

        // Validate
        ValidationResult validated = planner.validateAndGetTypeMetadata(sqlNode);

        sqlNode = validated.sqlNode();

        // Convert to Relational operators graph
        RelNode rel = planner.convert(sqlNode);

        // Transformation chain
        rel = planner.transform(PlannerPhase.HEURISTIC_OPTIMIZATION, rel.getTraitSet(), rel);

        RelTraitSet desired = rel.getCluster().traitSet()
            .replace(IgniteConvention.INSTANCE)
            .replace(IgniteDistributions.single())
            .simplify();

        IgniteRel igniteRel = planner.transform(PlannerPhase.OPTIMIZATION, desired, rel);

        // Split query plan to query fragments.
        List<Fragment> fragments = new Splitter().go(igniteRel);

        return new MultiStepPlanImpl(fragments, fieldsMetadata(validated, ctx));
    }

    /** */
    private FieldsQueryCursor<List<?>> executeSingle(UUID queryId, PlanningContext pctx, QueryPlan plan) {
        if (plan.type() == QueryPlan.Type.QUERY)
            return executeQuery(queryId, (MultiStepPlan) plan, pctx);

        throw new AssertionError("Unexpected plan type: " + plan);
    }

    /** */
    private FieldsQueryCursor<List<?>> executeQuery(UUID queryId, MultiStepPlan plan, PlanningContext pctx) {
        plan.init(mappingService(), pctx);

        List<Fragment> fragments = plan.fragments();

        // Local execution
        Fragment local = F.first(fragments);

        if (U.assertionsEnabled()) {
            assert local != null;

            NodesMapping mapping = local.mapping();

            assert mapping != null;

            List<UUID> nodes = mapping.nodes();

            assert nodes != null && nodes.size() == 1 && F.first(nodes).equals(pctx.localNodeId());
        }

        ExecutionContext ectx = new ExecutionContext(taskExecutor(), pctx, queryId, local.fragmentId(),
            local.mapping().partitions(pctx.localNodeId()), Commons.parametersMap(pctx.parameters()));

        Node<Object[]> node = new Implementor(partitionService(), mailboxRegistry(), exchangeService(), failureProcessor(), ectx, log).go(local.root());

        assert !(node instanceof SenderNode);

        QueryInfo info = new QueryInfo(ectx, fragments, node);

        // register query
        register(info);

        // start local execution
        info.consumer.request();

        // start remote execution
        if (fragments.size() > 1) {
            RelOp<IgniteRel, RelGraph> converter = new RelToGraphConverter();

            for (int i = 1; i < fragments.size(); i++) {
                Fragment fragment = fragments.get(i);

                boolean error = false;

                for (UUID nodeId : fragment.mapping().nodes()) {
                    if (error)
                        info.onResponse(nodeId, fragment.fragmentId(), new QueryCancelledException());
                    else {
                        try {
                            QueryStartRequest req = new QueryStartRequest(
                                queryId,
                                fragment.fragmentId(),
                                pctx.schemaName(),
                                converter.go(fragment.root()),
                                pctx.topologyVersion(),
                                fragment.mapping().partitions(nodeId),
                                pctx.parameters());

                            messageService().send(nodeId, req);
                        }
                        catch (Exception e) {
                            info.onResponse(nodeId, fragment.fragmentId(), e);
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

        return new ListFieldsQueryCursor<>(info.iterator(), Arrays::asList, plan.fieldsMetadata());
    }

    /** */
    private void register(QueryInfo info) {
        UUID queryId = info.ctx.queryId();
        PlanningContext pctx = info.ctx.parent();

        running.put(queryId, info);

        if (pctx.cancelGroup() == null || pctx.cancelGroup().add(info))
            return;

        running.remove(queryId);

        throw new IgniteSQLException(QueryCancelledException.ERR_MSG, IgniteQueryErrorCode.QUERY_CANCELED);
    }

    /** */
    private QueryCancelGroup cancelGroup(@Nullable QueryContext qryCtx) {
        GridQueryCancel cancel;

        if (qryCtx == null || (cancel = qryCtx.unwrap(GridQueryCancel.class)) == null)
            return null;

        return new QueryCancelGroup(cancel, failureProcessor());
    }

    /** */
    private List<GridQueryFieldMetadata> fieldsMetadata(ValidationResult validationResult, PlanningContext ctx) {
        List<RelDataTypeField> fields = validationResult.dataType().getFieldList();
        List<List<String>> origins = validationResult.origins();

        assert fields.size() == origins.size();

        ImmutableList.Builder<GridQueryFieldMetadata> b = ImmutableList.builder();

        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            List<String> origin = origins.get(i);

            b.add(new CalciteQueryFieldMetadata(
                F.isEmpty(origin) ? null : origin.get(0),
                F.isEmpty(origin) ? null : origin.get(1),
                field.getName(),
                String.valueOf(ctx.typeFactory().getJavaClass(field.getType())),
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
        RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(IgniteMetadata.METADATA_PROVIDER));
        try (IgnitePlanner planner = ctx.planner()) {
            IgniteRel root = planner.convert(msg.plan());

            assert root instanceof IgniteSender : root;

            ExecutionContext execCtx = new ExecutionContext(
                taskExecutor(),
                ctx,
                msg.queryId(),
                msg.fragmentId(),
                msg.partitions(),
                Commons.parametersMap(msg.parameters())
            );

            Node<Object[]> node = new Implementor(partitionService(), mailboxRegistry(), exchangeService(), failureProcessor(), execCtx, log).go(root);

            assert node instanceof Outbox : node;

            node.context().execute(node::request);

            messageService().send(nodeId, new QueryStartResponse(msg.queryId(), msg.fragmentId()));
        }
        catch (Exception ex) {
            cancelQuery(msg.queryId());

            if (ex instanceof ClusterTopologyCheckedException)
                return;

            try {
                messageService().send(nodeId, new QueryStartResponse(msg.queryId(), msg.fragmentId(), ex));
            }
            catch (IgniteCheckedException e) {
                e.addSuppressed(ex);

                U.warn(log, "Failed to send reply. [nodeId=" + nodeId + ']', e);
            }
        }
        finally {
            RelMetadataQuery.THREAD_PROVIDERS.remove();
        }
    }

    /** */
    private void onMessage(UUID nodeId, QueryCancelRequest msg) {
        assert nodeId != null && msg != null;

        cancelQuery(msg.queryId());
    }

    /** */
    private void onMessage(UUID nodeId, QueryStartResponse msg) {
        assert nodeId != null && msg != null;

        QueryInfo info = running.get(msg.queryId());

        if (info != null)
            info.onResponse(nodeId, msg.fragmentId(), msg.error());
    }

    /** */
    private void onConsumerClose(ConsumerNode consumer) {
        if (consumer.canceled())
            cancelQuery(consumer.queryId());
        else
            running.remove(consumer.queryId());
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
    private void executeCancel(Node<?> node) {
        node.context().execute(node::cancel);
    }

    /** */
    private enum QueryState {
        RUNNING, CANCELLING, CANCELLED
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
            int result = nodeId.hashCode();
            result = 31 * result + (int) (fragmentId ^ (fragmentId >>> 32));
            return result;
        }
    }

    /** */
    private final class QueryInfo implements QueryCancellable {
        /** */
        private final ExecutionContext ctx;

        /** */
        private final ConsumerNode consumer;

        /** remote nodes */
        private final Set<UUID> remotes;

        /** node to fragment */
        private final Set<RemoteFragmentKey> waiting;

        /** */
        private QueryState state;

        /** */
        private Throwable error;

        /** */
        private QueryInfo(ExecutionContext ctx, List<Fragment> fragments, Node<Object[]> root) {
            this.ctx = ctx;

            consumer = new ConsumerNode(ctx, root, ExecutionServiceImpl.this::onConsumerClose);

            remotes = new HashSet<>();
            waiting = new HashSet<>();

            for (int i = 1; i < fragments.size(); i++) {
                Fragment fragment = fragments.get(i);
                long id = fragment.fragmentId();
                List<UUID> nodes = fragment.mapping().nodes();

                remotes.addAll(nodes);

                for (UUID node : nodes)
                    waiting.add(new RemoteFragmentKey(node, id));
            }

            state = QueryState.RUNNING;
        }

        /** */
        public Iterator<Object[]> iterator() {
            return iteratorsHolder().iterator(consumer);
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

        /** */
        private void cancel() {
            boolean cancelLocal = false;
            boolean cancelRemote = false;
            QueryState state0 = null;

            synchronized (this) {
                if (state == QueryState.CANCELLED)
                    return;

                if (state == QueryState.RUNNING) {
                    cancelLocal = true;
                    state0 = state = QueryState.CANCELLING;
                }

                if (state == QueryState.CANCELLING && waiting.isEmpty()) {
                    cancelRemote = true;
                    state0 = state = QueryState.CANCELLED;
                }
            }

            if (cancelLocal)
                consumer.cancel();

            if (cancelRemote) {
                QueryCancelRequest msg = new QueryCancelRequest(ctx.queryId());

                for (UUID remote : remotes) {
                    try {
                        messageService().send(remote, msg);
                    }
                    catch (ClusterTopologyCheckedException e) {
                        U.warn(log, e.getMessage(), e);
                    }
                    catch (IgniteCheckedException e) {
                        throw U.convertException(e);
                    }
                }
            }

            if (state0 == QueryState.CANCELLED)
                running.remove(ctx.queryId());
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
    private static class OriginatingFilter implements Predicate<Node<?>> {
        /** */
        private final UUID nodeId;

        /** */
        private OriginatingFilter(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public boolean test(Node<?> node) {
            // Uninitialized inbox doesn't know originating node ID.
            return Objects.equals(node.context().originatingNodeId(), nodeId);
        }
    }
}
