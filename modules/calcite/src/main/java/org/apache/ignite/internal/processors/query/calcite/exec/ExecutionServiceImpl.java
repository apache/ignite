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

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.Query;
import org.apache.ignite.internal.processors.query.calcite.QueryRegistry;
import org.apache.ignite.internal.processors.query.calcite.RootQuery;
import org.apache.ignite.internal.processors.query.calcite.RunningFragment;
import org.apache.ignite.internal.processors.query.calcite.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
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
import org.apache.ignite.internal.processors.query.calcite.prepare.FieldsMetadata;
import org.apache.ignite.internal.processors.query.calcite.prepare.FieldsMetadataImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.FragmentPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCache;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.ListFieldsQueryCursor;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;
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
    private QueryRegistry qryReg;

    /** */
//    private final Map<UUID, QueryInfo> running;

    /** */
    private final RowHandler<Row> handler;

    /** */
    private final DdlCommandHandler ddlCmdHnd;

    /**
     * @param ctx Kernal.
     */
    public ExecutionServiceImpl(GridKernalContext ctx, RowHandler<Row> handler) {
        super(ctx);
        this.handler = handler;

        discoLsnr = (e, c) -> onNodeLeft(e.eventNode().id());
        //running = new ConcurrentHashMap<>();

        ddlCmdHnd = new DdlCommandHandler(
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

    /** */
    public QueryRegistry queryRegistry() {
        return qryReg;
    }

    /** */
    public void queryRegistry(QueryRegistry qryReg) {
        this.qryReg = qryReg;
    }

    /** {@inheritDoc} */
    @Override public void cancelQuery(UUID qryId) {
        Query qry = qryReg.query(qryId);

        if (qry != null)
            qry.cancel();
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
        queryRegistry(proc.queryRegistry());

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
                    .defaultSchema(schemaHolder().getDefaultSchema(schema))
                    .build()
            )
            .logger(log)
            .build();
    }

    /** */
    private QueryPlan prepareFragment(BaseQueryContext ctx, String jsonFragment) {
        return new FragmentPlan(fromJson(ctx, jsonFragment));
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> executePlan(
        RootQuery<Row> qry,
        QueryPlan plan,
        Object[] params
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
                return executeExplain((ExplainPlan)plan);

            case DDL:
                return executeDdl(qry, (DdlPlan)plan);

            default:
                throw new AssertionError("Unexpected plan type: " + plan);
        }
    }

    /** */
    private FieldsQueryCursor<List<?>> executeDdl(RootQuery qry, DdlPlan plan) {
        try {
            ddlCmdHnd.handle(qry.id(), plan.command());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSQLException("Failed to execute DDL statement [stmt=" + qry.sql() +
                ", err=" + e.getMessage() + ']', e);
        }
        finally {
            qryReg.unregister(qry.id());
        }

        return H2Utils.zeroCursor();
    }

    /** */
    private ListFieldsQueryCursor<?> mapAndExecutePlan(
        RootQuery<Row> qry,
        MultiStepPlan plan
    ) {
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
            Commons.parametersMap(qry.parameters()));

        Node<Row> node = new LogicalRelImplementor<>(ectx, partitionService(), mailboxRegistry(),
            exchangeService(), failureProcessor()).go(fragment.root());

        qry.run(ectx, plan, node);

//        QueryInfo info = new QueryInfo(ectx, plan, node);
//
//        // register query
//        register(info);

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
                    qry.onResponse(nodeId, fragment.fragmentId(), ex);
                else {
                    try {
                        QueryStartRequest req = new QueryStartRequest(
                            qry.id(),
                            qry.context().schemaName(),
                            fragment.serialized(),
                            ectx.topologyVersion(),
                            fragmentDesc,
                            qry.parameters());

                        messageService().send(nodeId, req);
                    }
                    catch (Throwable e) {
                        qry.onResponse(nodeId, fragment.fragmentId(), ex = e);
                    }
                }
            }
        }

        return new ListFieldsQueryCursor<>(plan, iteratorsHolder().iterator(qry.iterator()), ectx);
//        return new ListFieldsQueryCursor<>(plan, info.iterator(), ectx);
    }

    /** */
    private FieldsQueryCursor<List<?>> executeExplain(ExplainPlan plan) {
        QueryCursorImpl<List<?>> cur = new QueryCursorImpl<>(singletonList(singletonList(plan.plan())));
        cur.fieldsMeta(plan.fieldsMeta().queryFieldsMetadata(Commons.typeFactory()));

        return cur;
    }

    /** */
    private void executeFragment(Query qry, FragmentPlan plan, ExecutionContext<Row> ectx) {
        UUID origNodeId = ectx.originatingNodeId();

        Outbox<Row> node = new LogicalRelImplementor<>(
            ectx,
            partitionService(),
            mailboxRegistry(),
            exchangeService(),
            failureProcessor())
            .go(plan.root()
        );

        qry.addFragment(new RunningFragment<>(plan.root(), node, ectx));

        try {
            messageService().send(origNodeId, new QueryStartResponse(qry.id(), ectx.fragmentId()));
        }
        catch (IgniteCheckedException e) {
            IgniteException wrpEx = new IgniteException("Failed to send reply. [nodeId=" + origNodeId + ']', e);

            throw wrpEx;
        }

        node.init();
    }

    /** */
    private FieldsMetadata queryFieldsMetadata(PlanningContext ctx, RelDataType sqlType,
        @Nullable List<List<String>> origins) {
        RelDataType resultType = TypeUtils.getResultType(
            ctx.typeFactory(), ctx.catalogReader(), sqlType, origins);
        return new FieldsMetadataImpl(resultType, origins);
    }

    /** */
    private void onMessage(UUID nodeId, final QueryStartRequest msg) {
        assert nodeId != null && msg != null;

        try {
            Query<Row> qry = new Query<>(msg.queryId(), null, (q) -> qryReg.unregister(q.id()));

            qry = qryReg.register(qry);

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

                throw wrpEx;
            }

            throw ex;
        }
    }

    /** */
    private void onMessage(UUID nodeId, QueryStartResponse msg) {
        assert nodeId != null && msg != null;

        Query qry = qryReg.query(msg.queryId());

        if (qry != null) {
            assert qry instanceof RootQuery : "Unexpected query object: " + qry;

            ((RootQuery<Row>)qry).onResponse(nodeId, msg.fragmentId(), msg.error());
        }
    }

    /** */
    private void onMessage(UUID nodeId, ErrorMessage msg) {
        assert nodeId != null && msg != null;

        Query qry = qryReg.query(msg.queryId());

        if (qry != null) {
            assert qry instanceof RootQuery : "Unexpected query object: " + qry;

            ((RootQuery<Row>)qry).onError(new RemoteException(nodeId, msg.queryId(), msg.fragmentId(), msg.error()));
        }
    }

    /** */
    private void onNodeLeft(UUID nodeId) {
        qryReg.runningQueries().stream()
            .filter(q -> q instanceof RootQuery)
            .forEach((qry) -> ((RootQuery<Row>)qry).onNodeLeft(nodeId));
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

//    /** */
//    @SuppressWarnings("TypeMayBeWeakened")
//    private final class QueryInfo implements QueryCancellable {
//        /** */
//        private final ExecutionContext<Row> ctx;
//
//        /** */
//        private final RootNode<Row> root;
//
//        /** remote nodes */
//        private final Set<UUID> remotes;
//
//        /** node to fragment */
//        private final Set<RemoteFragmentKey> waiting;
//
//        /** */
//        private volatile QueryState state;
//
//        /** */
//        private QueryInfo(ExecutionContext<Row> ctx, MultiStepPlan plan, Node<Row> root) {
//            this.ctx = ctx;
//
//            RootNode<Row> rootNode = new RootNode<>(ctx, plan.fieldsMetadata().rowType(), this::tryClose);
//            rootNode.register(root);
//
//            this.root = rootNode;
//
//            remotes = new HashSet<>();
//            waiting = new HashSet<>();
//
//            for (int i = 1; i < plan.fragments().size(); i++) {
//                Fragment fragment = plan.fragments().get(i);
//                List<UUID> nodes = plan.mapping(fragment).nodeIds();
//
//                remotes.addAll(nodes);
//
//                for (UUID node : nodes)
//                    waiting.add(new RemoteFragmentKey(node, fragment.fragmentId()));
//            }
//
//            state = QueryState.RUNNING;
//        }
//
//        /** */
//        public Iterator<Row> iterator() {
//            return iteratorsHolder().iterator(root);
//        }
//
//        /** {@inheritDoc} */
//        @Override public void doCancel() {
//            root.close();
//        }
//
//        /**
//         * Can be called multiple times after receive each error at {@link #onResponse(RemoteFragmentKey, Throwable)}.
//         */
//        private void tryClose() {
//            QueryState state0 = null;
//
//            synchronized (this) {
//                if (state == QueryState.CLOSED)
//                    return;
//
//                if (state == QueryState.RUNNING)
//                    state0 = state = QueryState.CLOSING;
//
//                // 1) close local fragment
//                root.closeInternal();
//
//                if (state == QueryState.CLOSING && waiting.isEmpty())
//                    state0 = state = QueryState.CLOSED;
//            }
//
//            if (state0 == QueryState.CLOSED) {
//                // 2) unregister runing query
////                running.remove(ctx.queryId());
//
//                IgniteException wrpEx = null;
//
//                // 3) close remote fragments
//                for (UUID nodeId : remotes) {
//                    try {
//                        exchangeService().closeOutbox(nodeId, ctx.queryId(), -1, -1);
//                    }
//                    catch (IgniteCheckedException e) {
//                        if (wrpEx == null)
//                            wrpEx = new IgniteException("Failed to send cancel message. [nodeId=" + nodeId + ']', e);
//                        else
//                            wrpEx.addSuppressed(e);
//                    }
//                }
//
//                // 4) Cancel local fragment
//                root.context().execute(ctx::cancel, root::onError);
//
//                if (wrpEx != null)
//                    throw wrpEx;
//            }
//        }
//
//        /** */
//        private void onNodeLeft(UUID nodeId) {
//            List<RemoteFragmentKey> fragments = null;
//
//            synchronized (this) {
//                for (RemoteFragmentKey fragment : waiting) {
//                    if (!fragment.nodeId.equals(nodeId))
//                        continue;
//
//                    if (fragments == null)
//                        fragments = new ArrayList<>();
//
//                    fragments.add(fragment);
//                }
//            }
//
//            if (!F.isEmpty(fragments)) {
//                ClusterTopologyCheckedException ex = new ClusterTopologyCheckedException(
//                    "Failed to start query, node left. nodeId=" + nodeId);
//
//                for (RemoteFragmentKey fragment : fragments)
//                    onResponse(fragment, ex);
//            }
//        }
//
//        /** */
//        private void onResponse(UUID nodeId, long fragmentId, Throwable error) {
//            onResponse(new RemoteFragmentKey(nodeId, fragmentId), error);
//        }
//
//        /** */
//        private void onResponse(RemoteFragmentKey fragment, Throwable error) {
//            QueryState state;
//            synchronized (this) {
//                waiting.remove(fragment);
//                state = this.state;
//            }
//
//            if (error != null)
//                onError(error);
//            else if (state == QueryState.CLOSING)
//                tryClose();
//        }
//
//        /** */
//        private void onError(Throwable error) {
//            root.onError(error);
//
//            tryClose();
//        }
//    }
}
