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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryCancellable;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.exec.ConsumerNode;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.Implementor;
import org.apache.ignite.internal.processors.query.calcite.exec.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.apache.ignite.internal.processors.query.calcite.exec.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.Outbox;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.message.QueryCancelRequest;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartRequest;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartResponse;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.CacheKey;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerType;
import org.apache.ignite.internal.processors.query.calcite.prepare.Query;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryCache;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryCacheImpl;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.RelOp;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolderImpl;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelGraph;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelToGraphConverter;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.SenderNode;
import org.apache.ignite.internal.processors.query.calcite.splitter.Fragment;
import org.apache.ignite.internal.processors.query.calcite.splitter.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.splitter.Splitter;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.LifecycleAware;
import org.apache.ignite.internal.processors.query.calcite.util.ListFieldsQueryCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.igniteRel;

/**
 *
 */
public class CalciteQueryProcessor extends GridProcessorAdapter implements QueryEngine, MailboxRegistry {
    /** */
    public static final FrameworkConfig FRAMEWORK_CONFIG = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.configBuilder()
                // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
                // case when they are read, and whether identifiers are matched case-sensitively.
                .setLex(Lex.MYSQL)
                .build())
            // Dialects support.
            .operatorTable(SqlLibraryOperatorTableFactory.INSTANCE
                .getOperatorTable(
                    SqlLibrary.STANDARD,
                    SqlLibrary.MYSQL))
            // Context provides a way to store data within the planner session that can be accessed in planner rules.
            .context(Contexts.empty())
            // Custom cost factory to use during optimization
            .costFactory(null)
            .typeSystem(IgniteTypeSystem.INSTANCE)
            .build();

    /** */
    private final FrameworkConfig config;

    /** */
    private final QueryCache queryCache;

    /** */
    private final QueryTaskExecutor taskExecutor;

    /** */
    private final SchemaHolder schemaHolder;

    /** */
    private final MessageService messageService;

    /** */
    private final ExchangeService exchangeService;

    /** */
    private final MappingService mappingService;

    /** */
    private final Map<QueryKey, Outbox<?>> locals;

    /** */
    private final Map<QueryKey, Inbox<?>> remotes;

    /** */
    private final Map<UUID, QueryInfo> running;

    /**
     * @param ctx Kernal context.
     */
    public CalciteQueryProcessor(GridKernalContext ctx) {
        this(FRAMEWORK_CONFIG,
            ctx,
            new SchemaHolderImpl(ctx),
            new MessageServiceImpl(ctx),
            new QueryTaskExecutorImpl(ctx),
            new MappingServiceImpl(ctx),
            new QueryCacheImpl(ctx),
            new ExchangeServiceImpl(ctx));
    }

    /**
     * For tests purpose.
     * @param config Framework config.
     * @param ctx Kernal context.
     * @param schemaHolder Schema holder.
     * @param messageService Message service.
     * @param taskExecutor Task executor.
     * @param mappingService Mapping service.
     * @param queryCache Query cache;
     * @param exchangeService Exchange service.
     */
    CalciteQueryProcessor(FrameworkConfig config, GridKernalContext ctx, SchemaHolder schemaHolder, MessageService messageService,
        QueryTaskExecutor taskExecutor, MappingService mappingService, QueryCache queryCache, ExchangeServiceImpl exchangeService) {
        super(ctx);

        this.config = config;
        this.schemaHolder = schemaHolder;
        this.messageService = messageService;
        this.taskExecutor = taskExecutor;
        this.mappingService = mappingService;
        this.queryCache = queryCache;
        this.exchangeService = exchangeService;

        locals = new ConcurrentHashMap<>();
        remotes = new ConcurrentHashMap<>();
        running = new ConcurrentHashMap<>();
    }

    public QueryCache queryCache() {
        return queryCache;
    }

    public QueryTaskExecutor taskExecutor() {
        return taskExecutor;
    }

    public SchemaHolder schemaHolder() {
        return schemaHolder;
    }

    public MessageService messageService() {
        return messageService;
    }

    public MappingService mappingService() {
        return mappingService;
    }

    public ExchangeService exchangeService() {
        return exchangeService;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        onStart(ctx, queryCache, schemaHolder, messageService, mappingService, taskExecutor, exchangeService);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        onStop(queryCache, schemaHolder, messageService, mappingService, taskExecutor, exchangeService);
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> query(@Nullable QueryContext qryCtx, @Nullable String schemaName,
        String query, Object... params) throws IgniteSQLException {
        UUID queryId = UUID.randomUUID();

        IgniteCalciteContext cctx = createContext(qryCtx, schemaName, query, params);

        QueryPlan plan = queryCache.queryPlan(cctx, new CacheKey(schemaName, query), this::prepare);

        plan.init(cctx);

        // Local execution
        Fragment local = F.first(plan.fragments());

        if (U.assertionsEnabled()) {
            assert local != null;

            NodesMapping mapping = local.mapping();

            assert mapping != null;

            List<UUID> nodes = mapping.nodes();

            assert nodes != null && nodes.size() == 1 && F.first(nodes).equals(cctx.localNodeId());
        }

        ExecutionContext ectx = new ExecutionContext(
            cctx,
            queryId,
            local.fragmentId(),
            local.mapping().partitions(cctx.localNodeId()),
            Commons.parametersMap(params));

        Node<Object[]> node = new Implementor(ectx).go(igniteRel(local.root()));

        assert !(node instanceof SenderNode);

        QueryInfo info = new QueryInfo(ectx, local.root().getRowType(), new ConsumerNode(ectx, node, this::onClose));

        if (plan.fragments().size() == 1)
            running.put(queryId, info);
        else {
            // remote execution
            RelOp<IgniteRel, RelGraph> converter = new RelToGraphConverter();
            List<Pair<UUID, QueryStartRequest>> requests = new ArrayList<>();

            for (Fragment remote : plan.fragments().subList(1, plan.fragments().size())) {
                long id = remote.fragmentId();
                NodesMapping mapping = remote.mapping();
                RelGraph graph = converter.go(igniteRel(remote.root()));

                for (UUID nodeId : mapping.nodes()) {
                    info.addFragment(nodeId, id);

                    QueryStartRequest req = new QueryStartRequest(
                        queryId,
                        id,
                        schemaName,
                        graph,
                        cctx.topologyVersion(),
                        mapping.partitions(nodeId),
                        params);

                    requests.add(Pair.of(nodeId, req));
                }
            }

            running.put(queryId, info);

            // start remote execution
            for (Pair<UUID, QueryStartRequest> pair : requests)
                cctx.messageService().send(pair.left, pair.right);
        }

        // start local execution
        info.localNode.request();

        info.awaitAllReplies();

        return Collections.singletonList(
            new ListFieldsQueryCursor<>(
                info.type(),
                info.<Object[]>iterator(),
                Arrays::asList));
    }

    /** {@inheritDoc} */
    @Override public Inbox<?> register(Inbox<?> inbox) {
        Inbox<?> old = remotes.putIfAbsent(new QueryKey(inbox.queryId(), inbox.exchangeId()), inbox);

        return old != null ? old : inbox;
    }

    /** {@inheritDoc} */
    @Override public void unregister(Inbox<?> inbox) {
        remotes.remove(new QueryKey(inbox.queryId(), inbox.exchangeId()));
    }

    /** {@inheritDoc} */
    @Override public void register(Outbox<?> outbox) {
        Outbox<?> res = locals.put(new QueryKey(outbox.queryId(), outbox.exchangeId()), outbox);

        assert res == null : res;
    }

    /** {@inheritDoc} */
    @Override public void unregister(Outbox<?> outbox) {
        locals.remove(new QueryKey(outbox.queryId(), outbox.exchangeId()));
    }

    /** {@inheritDoc} */
    @Override public Outbox<?> outbox(UUID queryId, long exchangeId) {
        return locals.get(new QueryKey(queryId, exchangeId));
    }

    /** {@inheritDoc} */
    @Override public Inbox<?> inbox(UUID queryId, long exchangeId) {
        return remotes.get(new QueryKey(queryId, exchangeId));
    }

    /**
     * Starts query fragment execution routine.
     *
     * @param nodeId Client node ID.
     * @param queryId Query ID.
     * @param fragmentId Fragment ID.
     * @param schemaName Schema name.
     * @param topVer Topology version.
     * @param plan Query fragment plan.
     * @param parts Involved partitions.
     * @param params Query parameters.
     */
    public void executeFragment(UUID nodeId, UUID queryId, long fragmentId, String schemaName, AffinityTopologyVersion topVer,
        RelGraph plan, int[] parts, Object[] params) {

        IgniteCalciteContext ctx = createContext(schemaName, nodeId, topVer);

        Throwable err = null;

        try (IgnitePlanner planner = ctx.planner()) {
            RelNode root = planner.convert(plan);

            assert root instanceof IgniteSender : root;

            // TODO do we need a local optimisation phase here?

            ExecutionContext execCtx = new ExecutionContext(
                ctx,
                queryId,
                fragmentId,
                parts,
                Commons.parametersMap(params));

            Node<Object[]> node = new Implementor(execCtx).go(igniteRel(root));

            assert node instanceof Outbox : node;

            node.request();
        }
        catch (Throwable ex) {
            err = ex;

            throw ex;
        }
        finally {
            messageService().send(nodeId, new QueryStartResponse(queryId, fragmentId, err));
        }
    }

    public void onQueryStarted(UUID nodeId, UUID queryId, long fragmentId, Throwable error) {
        QueryInfo info = running.get(queryId);

        if (info != null)
            info.onResponse(nodeId, fragmentId, error);
    }

    public void cancel(UUID queryId) {
        for (Outbox<?> node : locals.values()) {
            if (node.queryId().equals(queryId))
                node.cancel();
        }

        QueryInfo info = running.get(queryId);

        if (info != null)
            info.cancel();
    }

    private void onClose(ConsumerNode consumer) {
        if (consumer.canceled())
            cancel(consumer.queryId());
        else
            running.remove(consumer.queryId());
    }

    /** */
    private QueryPlan prepare(IgniteCalciteContext ctx) {
        IgnitePlanner planner = ctx.planner();

        boolean processed = false;

        try {
            Query query = ctx.query();

            assert query != null;

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

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

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            // Split query plan to multi-step one.
            QueryPlan plan = new Splitter().go(igniteRel(rel));

            processed = true;

            return plan;
        }
        catch (SqlParseException e) {
            throw new IgniteSQLException("Failed to parse query.", IgniteQueryErrorCode.PARSING, e);
        }
        catch (ValidationException e) {
            throw new IgniteSQLException("Failed to validate query.", IgniteQueryErrorCode.UNKNOWN, e);
        }
        catch (Exception e) {
            throw new IgniteSQLException("Failed to plan query.", IgniteQueryErrorCode.UNKNOWN, e);
        }
        finally {
            if (!processed)
                U.closeQuiet(planner);
        }
    }

    /**
     * Creates a context for remote execution.
     *
     *
     * @param schemaName Schema name.
     * @param originatingNodeId Originating node ID (the node, who started the execution).
     * @param topVer Topology version.
     * @return Query execution context.
     */
    private IgniteCalciteContext createContext(String schemaName, UUID originatingNodeId, AffinityTopologyVersion topVer) {
        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE
            //, RelCollationTraitDef.INSTANCE TODO
        };

        return IgniteCalciteContext.builder()
            .localNodeId(ctx.localNodeId())
            .originatingNodeId(originatingNodeId)
            .kernalContext(ctx)
            .parentContext(config.getContext())
            .frameworkConfig(Frameworks.newConfigBuilder(config)
                .defaultSchema(schemaHolder.schema().getSubSchema(schemaName))
                .traitDefs(traitDefs)
                .build())
            .queryProcessor(this)
            .topologyVersion(topVer)
            .messageService(messageService)
            .mappingService(mappingService)
            .exchangeService(exchangeService)
            .taskExecutor(taskExecutor)
            .mailboxRegistry(this)
            .logger(log)
            .build();
    }

    /**
     * Creates a context for local planning.
     *
     * @param qryCtx External context.
     * @param query Query string.
     * @param params Query parameters.
     * @return Query execution context.
     */
    private IgniteCalciteContext createContext(@Nullable QueryContext qryCtx, @Nullable String schemaName, String query, Object[] params) {
        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE
            , DistributionTraitDef.INSTANCE
            //, RelCollationTraitDef.INSTANCE TODO
        };

        return IgniteCalciteContext.builder()
            .localNodeId(ctx.localNodeId())
            .kernalContext(ctx)
            .parentContext(Contexts.chain(Commons.convert(qryCtx), config.getContext()))
            .frameworkConfig(Frameworks.newConfigBuilder(config)
                .defaultSchema(schemaName != null ? schemaHolder.schema().getSubSchema(schemaName) : schemaHolder.schema())
                .traitDefs(traitDefs)
                .build())
            .queryProcessor(this)
            .query(new Query(query, params))
            .topologyVersion(readyAffinityVersion())
            .messageService(messageService)
            .mappingService(mappingService)
            .exchangeService(exchangeService)
            .taskExecutor(taskExecutor)
            .mailboxRegistry(this)
            .logger(log)
            .build();
    }

    /** */
    private AffinityTopologyVersion readyAffinityVersion() {
        return ctx.cache().context().exchange().readyAffinityVersion();
    }

    /** */
    private void onStart(GridKernalContext ctx, Object... components) {
        for (Object cmp : components) {
            if (cmp instanceof LifecycleAware)
                ((LifecycleAware) cmp).onStart(ctx);
        }
    }

    /** */
    private void onStop(Object... components) {
        for (Object cmp : components) {
            if (cmp instanceof LifecycleAware)
                ((LifecycleAware) cmp).onStop();
        }
    }

    /** */
    private enum QueryState {
        RUNNING, CANCELLING, CANCELLED
    }

    /** */
    private static class QueryKey {
        private final UUID id;
        private final long cntr;

        private QueryKey(UUID id, long cntr) {
            this.id = id;
            this.cntr = cntr;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            QueryKey that = (QueryKey) o;

            if (cntr != that.cntr)
                return false;
            return id.equals(that.id);
        }

        @Override public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + (int) (cntr ^ (cntr >>> 32));
            return result;
        }
    }

    /** */
    private final class QueryInfo implements QueryCancellable {
        /** */
        private final ExecutionContext ctx;

        /** */
        private final RelDataType type;

        /** */
        private final ConsumerNode localNode;

        /** remote nodes */
        private final Set<UUID> remotes;

        /** node to fragment */
        private final Set<QueryKey> waiting;

        /** */
        private QueryState state;

        /** */
        private Throwable error;

        /** */
        private QueryInfo(ExecutionContext ctx, RelDataType type, ConsumerNode localNode) {
            this.ctx = ctx;
            this.type = type;
            this.localNode = localNode;

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
            return localNode;
        }

        /** */
        private RelDataType type() {
            return type;
        }

        /** */
        private <T> Iterator<T> iterator() {
            return (Iterator<T>) localNode;
        }

        /** */
        private void addFragment(UUID nodeId, long fragmentId) {
            remotes.add(nodeId);
            waiting.add(new QueryKey(nodeId, fragmentId));
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
                localNode.cancel();

            if (cancelRemote)
                ctx.parent().messageService().send(remotes, new QueryCancelRequest(ctx.queryId()));

            if (state0 == QueryState.CANCELLED)
                running.remove(ctx.queryId());
        }

        /** */
        private void onNodeLeft(UUID nodeId) {
            List<QueryKey> fragments = null;

            synchronized (this) {
                for (QueryKey fragment : waiting) {
                    if (!fragment.id.equals(nodeId))
                        continue;

                    if (fragments == null)
                        fragments = new ArrayList<>();

                    fragments.add(fragment);
                }
            }

            if (!F.isEmpty(fragments)) {
                ClusterTopologyCheckedException ex = new ClusterTopologyCheckedException("Failed to start query, node left. nodeId=" + nodeId);

                for (QueryKey fragment : fragments)
                    onResponse(fragment, ex);
            }
        }

        /** */
        private void onResponse(UUID nodeId, long fragmentId, Throwable error) {
            onResponse(new QueryKey(nodeId, fragmentId), error);
        }

        /** */
        private void onResponse(QueryKey fragment, Throwable error) {
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
