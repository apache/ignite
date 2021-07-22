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

import java.util.List;

import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.DummyPlanCache;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolderImpl;
import org.apache.ignite.internal.processors.query.calcite.util.StripedThreadPoolExecutor;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SqlQueryProcessor {
    /** Default Ignite thread keep alive time. */
    public static final long DFLT_THREAD_KEEP_ALIVE_TIME = 60_000L;

    private final ExecutionService executionSrvc;

    private final MessageService msgSrvc;

    private final QueryTaskExecutor taskExecutor;

    public SqlQueryProcessor(
        ClusterService clusterSrvc,
        TableManager tableManager
    ) {
        taskExecutor = new QueryTaskExecutorImpl(
            new StripedThreadPoolExecutor(
                4,
                "calciteQry",
                null,
                true,
                DFLT_THREAD_KEEP_ALIVE_TIME
            )
        );

        msgSrvc = new MessageServiceImpl(
            clusterSrvc.topologyService(),
            clusterSrvc.messagingService(),
            taskExecutor
        );

        SchemaHolderImpl schemaHolder = new SchemaHolderImpl(clusterSrvc.topologyService());

        executionSrvc = new ExecutionServiceImpl<>(
            clusterSrvc.topologyService(),
            msgSrvc,
            new DummyPlanCache(),
            schemaHolder,
            taskExecutor,
            ArrayRowHandler.INSTANCE
        );

        tableManager.listen(TableEvent.CREATE, new TableCreatedListener(schemaHolder));
        tableManager.listen(TableEvent.ALTER, new TableUpdatedListener(schemaHolder));
        tableManager.listen(TableEvent.DROP, new TableDroppedListener(schemaHolder));
    }

    public List<Cursor<List<?>>> query(String schemaName, String qry, Object... params) {
        return executionSrvc.executeQuery(schemaName, qry, params);
    }

    private abstract static class AbstractTableEventListener implements EventListener<TableEventParameters> {
        protected final SchemaHolderImpl schemaHolder;

        private AbstractTableEventListener(
            SchemaHolderImpl schemaHolder
        ) {
            this.schemaHolder = schemaHolder;
        }

        /** {@inheritDoc} */
        @Override public void remove(@NotNull Throwable exception) {
            throw new IllegalStateException();
        }
    }

    private static class TableCreatedListener extends AbstractTableEventListener {
        private TableCreatedListener(
            SchemaHolderImpl schemaHolder
        ) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onSqlTypeCreated(
                "PUBLIC",
                parameters.tableName(),
                parameters.table().schemaView().schema()
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
        @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onSqlTypeUpdated(
                "PUBLIC",
                parameters.tableName(),
                parameters.table().schemaView().schema()
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
        @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onSqlTypeDropped(
                "PUBLIC",
                parameters.tableName()
            );

            return false;
        }
    }
}
