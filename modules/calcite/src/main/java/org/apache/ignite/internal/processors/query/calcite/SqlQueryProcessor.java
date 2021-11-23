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
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.extension.SqlExtension;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCache;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCacheImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolderImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *  SqlQueryProcessor.
 *  TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class SqlQueryProcessor implements QueryProcessor {
    /** Size of the cache for query plans. */
    public static final int PLAN_CACHE_SIZE = 1024;

    private final ClusterService clusterSrvc;

    private final TableManager tableManager;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Keeps queries plans to avoid expensive planning of the same queries. */
    private final QueryPlanCache planCache = new QueryPlanCacheImpl(PLAN_CACHE_SIZE);

    /** Event listeners to close. */
    private final List<Pair<TableEvent, EventListener<TableEventParameters>>> evtLsnrs = new ArrayList<>();

    private volatile ExecutionService executionSrvc;

    private volatile MessageService msgSrvc;

    private volatile QueryTaskExecutor taskExecutor;

    private volatile Map<String, SqlExtension> extensions;

    public SqlQueryProcessor(
            ClusterService clusterSrvc,
            TableManager tableManager
    ) {
        this.clusterSrvc = clusterSrvc;
        this.tableManager = tableManager;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        taskExecutor = new QueryTaskExecutorImpl(clusterSrvc.localConfiguration().getName());

        msgSrvc = new MessageServiceImpl(
                clusterSrvc.topologyService(),
                clusterSrvc.messagingService(),
                taskExecutor
        );

        List<SqlExtension> extensionList = new ArrayList<>();

        ServiceLoader<SqlExtension> loader = ServiceLoader.load(SqlExtension.class);

        loader.reload();

        loader.forEach(extensionList::add);

        extensions = extensionList.stream().collect(Collectors.toMap(SqlExtension::name, Function.identity()));

        SchemaHolderImpl schemaHolder = new SchemaHolderImpl(planCache::clear);

        executionSrvc = new ExecutionServiceImpl<>(
                clusterSrvc.topologyService(),
                msgSrvc,
                planCache,
                schemaHolder,
                taskExecutor,
                ArrayRowHandler.INSTANCE,
                extensions
        );

        registerTableListener(TableEvent.CREATE, new TableCreatedListener(schemaHolder));
        registerTableListener(TableEvent.ALTER, new TableUpdatedListener(schemaHolder));
        registerTableListener(TableEvent.DROP, new TableDroppedListener(schemaHolder));

        taskExecutor.start();
        msgSrvc.start();
        executionSrvc.start();
        planCache.start();

        extensionList.forEach(ext -> ext.init(catalog -> schemaHolder.registerExternalCatalog(ext.name(), catalog)));
    }

    private void registerTableListener(TableEvent evt, AbstractTableEventListener lsnr) {
        evtLsnrs.add(Pair.of(evt, lsnr));

        tableManager.listen(evt, lsnr);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        busyLock.block();

        List<AutoCloseable> toClose = new ArrayList<>();

        Map<String, SqlExtension> extensions = this.extensions;
        if (extensions != null) {
            toClose.addAll(
                    extensions.values().stream()
                            .map(ext -> (AutoCloseable) ext::stop)
                            .collect(Collectors.toList())
            );
        }

        Stream<AutoCloseable> closableComponents = Stream.of(
                executionSrvc::stop,
                msgSrvc::stop,
                taskExecutor::stop,
                planCache::stop
        );

        Stream<AutoCloseable> closableListeners = evtLsnrs.stream()
                .map((p) -> () -> tableManager.removeListener(p.left, p.right));

        toClose.addAll(
                Stream.concat(closableComponents, closableListeners).collect(Collectors.toList())
        );

        IgniteUtils.closeAll(toClose);
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlCursor<List<?>>> query(String schemaName, String qry, Object... params) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return executionSrvc.executeQuery(schemaName, qry, params);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private abstract static class AbstractTableEventListener implements EventListener<TableEventParameters> {
        protected final SchemaHolderImpl schemaHolder;

        private AbstractTableEventListener(
                SchemaHolderImpl schemaHolder
        ) {
            this.schemaHolder = schemaHolder;
        }

        /** {@inheritDoc} */
        @Override
        public void remove(@NotNull Throwable exception) {
            // No-op.
        }
    }

    private static class TableCreatedListener extends AbstractTableEventListener {
        private TableCreatedListener(
                SchemaHolderImpl schemaHolder
        ) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onSqlTypeCreated(
                    "PUBLIC",
                    parameters.table()
            );

            return false;
        }
    }

    private static class TableUpdatedListener extends AbstractTableEventListener {
        private TableUpdatedListener(
                SchemaHolderImpl schemaHolder
        ) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onSqlTypeUpdated(
                    "PUBLIC",
                    parameters.table()
            );

            return false;
        }
    }

    private static class TableDroppedListener extends AbstractTableEventListener {
        private TableDroppedListener(
                SchemaHolderImpl schemaHolder
        ) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onSqlTypeDropped(
                    "PUBLIC",
                    parameters.tableName()
            );

            return false;
        }
    }
}
