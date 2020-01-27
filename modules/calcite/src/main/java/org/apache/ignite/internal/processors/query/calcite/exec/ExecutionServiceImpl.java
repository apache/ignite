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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryCancellable;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.message.QueryCancelRequest;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartRequest;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartResponse;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.PartitionService;
import org.apache.ignite.internal.processors.query.calcite.prepare.CacheKey;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerType;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCache;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.RelOp;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelGraph;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelToGraphConverter;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.SenderNode;
import org.apache.ignite.internal.processors.query.calcite.splitter.Fragment;
import org.apache.ignite.internal.processors.query.calcite.splitter.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.splitter.Splitter;
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
     * @param queryPlanCache Query cache.
     */
    public void queryPlanCache(QueryPlanCache queryPlanCache) {
        this.queryPlanCache = queryPlanCache;
    }

    /**
     * @param schemaHolder Schema holder.
     */
    public void schemaHolder(SchemaHolder schemaHolder) {
        this.schemaHolder = schemaHolder;
    }

    /**
     * @param taskExecutor Task executor.
     */
    public void taskExecutor(QueryTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    /**
     * @param failureProcessor Failure processor.
     */
    public void failureProcessor(FailureProcessor failureProcessor) {
        this.failureProcessor = failureProcessor;
    }

    /**
     * @param partitionService Partition service.
     */
    public void partitionService(PartitionService partitionService) {
        this.partitionService = partitionService;
    }

    /**
     * @param mailboxRegistry Mailbox registry.
     */
    public void mailboxRegistry(MailboxRegistry mailboxRegistry) {
        this.mailboxRegistry = mailboxRegistry;
    }

    /**
     * @param mappingService Mapping service.
     */
    public void mappingService(MappingService mappingService) {
        this.mappingService = mappingService;
    }

    /**
     * @param messageService Message service.
     */
    public void messageService(MessageService messageService) {
        this.messageService = messageService;
    }

    /**
     * @param exchangeService Exchange service.
     */
    public void exchangeService(ExchangeService exchangeService) {
        this.exchangeService = exchangeService;
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> executeQuery(@Nullable QueryContext ctx, String schema, String query, Object[] params) {
        UUID queryId = UUID.randomUUID();

        PlanningContext pctx = createContext(ctx, schema, query, params);

        QueryPlan plan = prepare(pctx);

        // Local execution
        Fragment local = F.first(plan.fragments());

        if (U.assertionsEnabled()) {
            assert local != null;

            NodesMapping mapping = local.mapping();

            assert mapping != null;

            List<UUID> nodes = mapping.nodes();

            assert nodes != null && nodes.size() == 1 && F.first(nodes).equals(pctx.localNodeId());
        }

        ExecutionContext ectx = new ExecutionContext(
            taskExecutor, pctx, queryId, local.fragmentId(), local.mapping().partitions(pctx.localNodeId()), Commons.parametersMap(params)
        );

        Node<Object[]> node = new Implementor(partitionService, mailboxRegistry, exchangeService, failureProcessor, ectx, log).go(local.root());

        assert !(node instanceof SenderNode);

        QueryInfo info = new QueryInfo(ectx, local.root().getRowType(), new ConsumerNode(ectx, node, this::onConsumerClose));

        if (plan.fragments().size() == 1)
            running.put(queryId, info);
        else {
            // remote execution
            RelOp<IgniteRel, RelGraph> converter = new RelToGraphConverter();
            List<Pair<UUID, QueryStartRequest>> requests = new ArrayList<>();

            for (Fragment remote : plan.fragments().subList(1, plan.fragments().size())) {
                long id = remote.fragmentId();
                NodesMapping mapping = remote.mapping();
                RelGraph graph = converter.go(remote.root());

                for (UUID nodeId : mapping.nodes()) {
                    info.addFragment(nodeId, id);

                    QueryStartRequest req = new QueryStartRequest(
                        queryId,
                        id,
                        schema,
                        graph,
                        pctx.topologyVersion(),
                        mapping.partitions(nodeId),
                        params);

                    requests.add(Pair.of(nodeId, req));
                }
            }

            running.put(queryId, info);

            // start remote execution
            for (Pair<UUID, QueryStartRequest> pair : requests)
                messageService.send(pair.left, pair.right);
        }

        // start local execution
        info.consumer.request();

        info.awaitAllReplies();

        // TODO weak map to stop query on cursor collected by GC.
        return new ListFieldsQueryCursor<>(info.type(), info.<Object[]>iterator(), Arrays::asList);
    }

    /** {@inheritDoc} */
    @Override public void cancelQuery(UUID queryId) {
        mailboxRegistry.outboxes(queryId).forEach(Outbox::cancel);

        QueryInfo info = running.get(queryId);

        if (info != null)
            info.cancel();
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
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

        registerListeners();

        Optional.ofNullable(ctx.event()).ifPresent(this::registerDiscovery);
     }

    /**
     * For tests purpose.
     */
    public void registerListeners() {
        messageService.register((n,m) -> onMessage(n, (QueryStartRequest) m), MessageType.QUERY_START_REQUEST);
        messageService.register((n,m) -> onMessage(n, (QueryStartResponse) m), MessageType.QUERY_START_RESPONSE);
        messageService.register((n,m) -> onMessage(n, (QueryCancelRequest) m), MessageType.QUERY_CANCEL_REQUEST);
    }

    /** {@inheritDoc} */
    @Override public void onStop() {
        Optional.ofNullable(ctx.event()).ifPresent(this::unregisterDiscovery);
        running.clear();
    }

    /** */
    private void registerDiscovery(GridEventStorageManager mgr) {
        mgr.addDiscoveryEventListener(discoLsnr, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
    }

    /** */
    private void unregisterDiscovery(GridEventStorageManager mgr) {
        mgr.removeDiscoveryEventListener(discoLsnr, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
    }

    /** */
    private QueryPlan prepare(PlanningContext ctx) {
        CacheKey cacheKey = new CacheKey(ctx.schema().getName(), ctx.query());

        QueryPlan plan = queryPlanCache.queryPlan(ctx, cacheKey, this::prepare0);

        plan.init(mappingService, ctx);

        return plan;
    }

    /** */
    private QueryPlan prepare0(PlanningContext ctx) {
        IgnitePlanner planner = ctx.planner();

        try {
            String query = ctx.query();

            assert query != null;

            // Parse
            SqlNode sqlNode = planner.parse(query);

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            RelNode rel = planner.convert(sqlNode);

            // Transformation chain
            rel = planner.transform(PlannerType.HEP, PlannerPhase.SUBQUERY_REWRITE, rel, rel.getTraitSet());

            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .simplify();

            IgniteRel igniteRel = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            // Split query plan to multi-step one.
            return new Splitter().go(igniteRel);
        }
        catch (SqlParseException e) {
            IgniteSQLException ex = new IgniteSQLException("Failed to parse query.", IgniteQueryErrorCode.PARSING, e);
            Commons.close(planner, ex);
            throw ex;
        }
        catch (ValidationException e) {
            IgniteSQLException ex = new IgniteSQLException("Failed to validate query.", IgniteQueryErrorCode.UNKNOWN, e);
            Commons.close(planner, ex);
            throw ex;
        }
        catch (Exception e) {
            IgniteSQLException ex = new IgniteSQLException("Failed to plan query.", IgniteQueryErrorCode.UNKNOWN, e);
            Commons.close(planner, ex);
            throw ex;
        }
    }

    /** */
    private PlanningContext createContext(@Nullable QueryContext qryCtx, @Nullable String schemaName, String query, Object[] params) {
        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE
            , DistributionTraitDef.INSTANCE
            //, RelCollationTraitDef.INSTANCE TODO
        };

        return PlanningContext.builder()
            .localNodeId(ctx.localNodeId())
            .parentContext(Commons.convert(qryCtx)) // TODO Connection config on the basis of query context
            .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schemaName != null
                    ? schemaHolder.schema().getSubSchema(schemaName)
                    : schemaHolder.schema())
                .traitDefs(traitDefs)
                .build())
            .query(query)
            .parameters(params)
            .topologyVersion(currentTopologyVersion())
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
            .localNodeId(ctx.localNodeId())
            .originatingNodeId(originatingNodeId)
            .parentContext(Contexts.empty())
            .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schemaName != null
                    ? schemaHolder.schema().getSubSchema(schemaName)
                    : schemaHolder.schema())
                .traitDefs(traitDefs)
                .build())
            .topologyVersion(topVer)
            .logger(log)
            .build();
    }

    /** */
    private AffinityTopologyVersion currentTopologyVersion() {
        return ctx.cache().context().exchange().readyAffinityVersion();
    }

    /** */
    private void onMessage(UUID nodeId, QueryStartRequest msg) {
        assert nodeId != null && msg != null;

        PlanningContext ctx = createContext(msg.schema(), nodeId, msg.topologyVersion());

        try (IgnitePlanner planner = ctx.planner()) {
            IgniteRel root = planner.convert(msg.plan());

            assert root instanceof IgniteSender : root;

            // TODO do we need a local optimisation phase here?
            ExecutionContext execCtx = new ExecutionContext(
                taskExecutor,
                ctx,
                msg.queryId(),
                msg.fragmentId(),
                msg.partitions(),
                Commons.parametersMap(msg.parameters())
            );

            Node<Object[]> node = new Implementor(partitionService, mailboxRegistry, exchangeService, failureProcessor, execCtx, log).go(root);

            assert node instanceof Outbox : node;

            node.request();

            messageService.send(nodeId, new QueryStartResponse(msg.queryId(), msg.fragmentId()));
        }
        catch (Exception ex) {
            messageService.send(nodeId, new QueryStartResponse(msg.queryId(), msg.fragmentId(), ex));
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
    }

    /** */
    private static class RemoteFragmentKey {
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
    private enum QueryState {
        RUNNING, CANCELLING, CANCELLED
    }

    /** */
    private final class QueryInfo implements QueryCancellable {
        /** */
        private final ExecutionContext ctx;

        /** */
        private final RelDataType type;

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
        private QueryInfo(ExecutionContext ctx, RelDataType type, ConsumerNode consumer) {
            this.ctx = ctx;
            this.type = type;
            this.consumer = consumer;

            remotes = new HashSet<>();
            waiting = new HashSet<>();

            state = QueryState.RUNNING;
        }

        /** {@inheritDoc} */
        @Override public void doCancel() {
            cancel();
        }

        /** */
        private ConsumerNode localNode() {
            return consumer;
        }

        /** */
        private RelDataType type() {
            return type;
        }

        /** */
        private <T> Iterator<T> iterator() {
            return (Iterator<T>) consumer;
        }

        /** */
        private void addFragment(UUID nodeId, long fragmentId) {
            remotes.add(nodeId);
            waiting.add(new RemoteFragmentKey(nodeId, fragmentId));
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

            if (cancelRemote)
                messageService.send(remotes, new QueryCancelRequest(ctx.queryId()));

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
}
