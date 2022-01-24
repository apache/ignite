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

package org.apache.ignite.internal.sql.engine.exec.ddl;

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.PrimaryKeyView;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.internal.schema.definition.TableDefinitionImpl;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AbstractTableDdlCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableAddCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableDropCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.ColumnDefinition;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropTableCommand;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.IgniteObjectName;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.builder.ColumnDefinitionBuilder;
import org.apache.ignite.schema.definition.builder.PrimaryKeyDefinitionBuilder;
import org.apache.ignite.schema.definition.builder.SortedIndexDefinitionBuilder;
import org.apache.ignite.schema.definition.builder.SortedIndexDefinitionBuilder.SortedIndexColumnBuilder;

/** DDL commands handler. */
public class DdlCommandHandler {
    private final TableManager tableManager;

    public DdlCommandHandler(TableManager tblManager) {
        tableManager = tblManager;
    }

    /** Planning context. */
    private PlanningContext pctx;

    /** Handles ddl commands. */
    public void handle(DdlCommand cmd, PlanningContext pctx) throws IgniteInternalCheckedException {
        validateCommand(cmd);

        this.pctx = pctx;

        if (cmd instanceof CreateTableCommand) {
            handleCreateTable((CreateTableCommand) cmd);
        } else if (cmd instanceof DropTableCommand) {
            handleDropTable((DropTableCommand) cmd);
        } else if (cmd instanceof AlterTableAddCommand) {
            handleAlterAddColumn((AlterTableAddCommand) cmd);
        } else if (cmd instanceof AlterTableDropCommand) {
            handleAlterDropColumn((AlterTableDropCommand) cmd);
        } else if (cmd instanceof CreateIndexCommand) {
            handleCreateIndex((CreateIndexCommand) cmd);
        } else if (cmd instanceof DropIndexCommand) {
            handleDropIndex((DropIndexCommand) cmd);
        } else {
            throw new IgniteInternalCheckedException("Unsupported DDL operation ["
                    + "cmdName=" + (cmd == null ? null : cmd.getClass().getSimpleName()) + "; "
                    + "querySql=\"" + pctx.query() + "\"]");
        }
    }

    /** Validate command. */
    private void validateCommand(DdlCommand cmd) {
        if (cmd instanceof AbstractTableDdlCommand) {
            AbstractTableDdlCommand cmd0 = (AbstractTableDdlCommand) cmd;

            if (IgniteUtils.nullOrEmpty(cmd0.tableName())) {
                throw new IllegalArgumentException("Table name is undefined.");
            }
        }
    }

    /** Handles create table command. */
    private void handleCreateTable(CreateTableCommand cmd) {
        final PrimaryKeyDefinitionBuilder pkeyDef = SchemaBuilders.primaryKey();

        pkeyDef.withColumns(IgniteObjectName.quoteNames(cmd.primaryKeyColumns()));
        pkeyDef.withAffinityColumns(IgniteObjectName.quoteNames(cmd.affColumns()));

        final IgniteTypeFactory typeFactory = pctx.typeFactory();

        final List<org.apache.ignite.schema.definition.ColumnDefinition> colsInner = new ArrayList<>();

        for (ColumnDefinition col : cmd.columns()) {
            ColumnDefinitionBuilder col0 = SchemaBuilders.column(
                            IgniteObjectName.quote(col.name()),
                            typeFactory.columnType(col.type())
                    )
                    .asNullable(col.nullable())
                    .withDefaultValueExpression(col.defaultValue());

            colsInner.add(col0.build());
        }

        Consumer<TableChange> tblChanger = tblCh -> {
            TableChange conv = convert(SchemaBuilders.tableBuilder(
                            IgniteObjectName.quote(cmd.schemaName()),
                            IgniteObjectName.quote(cmd.tableName())
                    )
                    .columns(colsInner)
                    .withPrimaryKey(pkeyDef.build()).build(), tblCh);

            if (cmd.partitions() != null) {
                conv.changePartitions(cmd.partitions());
            }

            if (cmd.replicas() != null) {
                conv.changeReplicas(cmd.replicas());
            }
        };

        String fullName = TableDefinitionImpl.canonicalName(
                IgniteObjectName.quote(cmd.schemaName()),
                IgniteObjectName.quote(cmd.tableName())
        );

        try {
            tableManager.createTable(fullName, tblChanger);
        } catch (TableAlreadyExistsException ex) {
            if (!cmd.ifTableExists()) {
                throw ex;
            }
        }
    }

    /** Handles drop table command. */
    private void handleDropTable(DropTableCommand cmd) {
        String fullName = TableDefinitionImpl.canonicalName(
                IgniteObjectName.quote(cmd.schemaName()),
                IgniteObjectName.quote(cmd.tableName())
        );
        try {
            tableManager.dropTable(fullName);
        } catch (TableNotFoundException ex) {
            if (!cmd.ifTableExists()) {
                throw ex;
            }
        }
    }

    /** Handles add column command. */
    private void handleAlterAddColumn(AlterTableAddCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return;
        }

        String fullName = TableDefinitionImpl.canonicalName(
                IgniteObjectName.quote(cmd.schemaName()),
                IgniteObjectName.quote(cmd.tableName())
        );

        try {
            addColumnInternal(fullName, cmd.columns(), cmd.ifColumnNotExists());
        } catch (TableNotFoundException ex) {
            if (!cmd.ifTableExists()) {
                throw ex;
            }
        }
    }

    /** Handles drop column command. */
    private void handleAlterDropColumn(AlterTableDropCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return;
        }

        String fullName = TableDefinitionImpl.canonicalName(
                IgniteObjectName.quote(cmd.schemaName()),
                IgniteObjectName.quote(cmd.tableName())
        );

        try {
            dropColumnInternal(fullName, cmd.columns(), cmd.ifColumnExists());
        } catch (TableNotFoundException ex) {
            if (!cmd.ifTableExists()) {
                throw ex;
            }
        }
    }

    /** Handles create index command. */
    private void handleCreateIndex(CreateIndexCommand cmd) {
        // Only sorted idx for now.
        SortedIndexDefinitionBuilder idx = SchemaBuilders.sortedIndex(cmd.indexName());

        for (Pair<String, Boolean> idxInfo : cmd.columns()) {
            SortedIndexColumnBuilder idx0 = idx.addIndexColumn(idxInfo.getFirst());

            if (idxInfo.getSecond()) {
                idx0.desc();
            }

            idx0.done();
        }

        String fullName = TableDefinitionImpl.canonicalName(
                IgniteObjectName.quote(cmd.schemaName()),
                IgniteObjectName.quote(cmd.tableName())
        );

        tableManager.alterTable(fullName, chng -> chng.changeIndices(idxes -> {
            if (idxes.get(cmd.indexName()) != null) {
                if (!cmd.ifIndexNotExists()) {
                    throw new IndexAlreadyExistsException(cmd.indexName());
                } else {
                    return;
                }
            }

            idxes.create(cmd.indexName(), tableIndexChange -> convert(idx.build(), tableIndexChange));
        }));
    }

    /** Handles drop index command. */
    private void handleDropIndex(DropIndexCommand cmd) {
        throw new UnsupportedOperationException("DROP INDEX command not supported for now.");
    }

    /**
     * Adds a column according to the column definition.
     *
     * @param fullName Table with schema name.
     * @param colsDef  Columns defenitions.
     * @param colNotExist Flag indicates exceptionally behavior in case of already existing column.
     */
    private void addColumnInternal(String fullName, List<ColumnDefinition> colsDef, boolean colNotExist) {
        tableManager.alterTable(
                fullName,
                chng -> chng.changeColumns(cols -> {
                    Map<String, String> colNamesToOrders = columnOrdersToNames(chng.columns());

                    List<ColumnDefinition> colsDef0;

                    if (!colNotExist) {
                        colsDef.stream().filter(k -> colNamesToOrders.containsKey(k.name())).findAny()
                                .ifPresent(c -> {
                                    throw new ColumnAlreadyExistsException(c.name());
                                });

                        colsDef0 = colsDef;
                    } else {
                        colsDef0 = colsDef.stream().filter(k -> !colNamesToOrders.containsKey(k.name())).collect(Collectors.toList());
                    }

                    final IgniteTypeFactory typeFactory = pctx.typeFactory();

                    for (ColumnDefinition col : colsDef0) {
                        ColumnDefinitionBuilder col0 = SchemaBuilders.column(
                                        IgniteObjectName.quote(col.name()),
                                        typeFactory.columnType(col.type())
                                )
                                .asNullable(col.nullable())
                                .withDefaultValueExpression(col.defaultValue());

                        cols.create(col.name(), colChg -> convert(col0.build(), colChg));
                    }
                }));
    }

    /**
     * Drops a column(s) exceptional behavior depends on {@code colExist} flag.
     *
     * @param fullName Table with schema name.
     * @param colNames Columns definitions.
     * @param colExist Flag indicates exceptionally behavior in case of already existing column.
     */
    private void dropColumnInternal(String fullName, Set<String> colNames, boolean colExist) {
        tableManager.alterTable(
                fullName,
                chng -> chng.changeColumns(cols -> {
                    PrimaryKeyView priKey = chng.primaryKey();

                    Map<String, String> colNamesToOrders = columnOrdersToNames(chng.columns());

                    Set<String> colNames0 = new HashSet<>();

                    Set<String> primaryCols = Set.of(priKey.columns());

                    for (String colName : colNames) {
                        if (!colNamesToOrders.containsKey(colName)) {
                            if (!colExist) {
                                throw new ColumnNotFoundException(colName, fullName);
                            }
                        } else {
                            colNames0.add(colName);
                        }

                        if (primaryCols.contains(colName)) {
                            throw new IgniteException(IgniteStringFormatter
                                    .format("Can`t delete column, belongs to primary key: [name={}]", colName));
                        }
                    }

                    colNames0.forEach(k -> cols.delete(colNamesToOrders.get(k)));
                }));
    }

    /** Map column name to order. */
    private static Map<String, String> columnOrdersToNames(NamedListView<? extends ColumnView> cols) {
        Map<String, String> colNames = new HashMap<>(cols.size());

        for (String colOrder : cols.namedListKeys()) {
            colNames.put(cols.get(colOrder).name(), colOrder);
        }

        return colNames;
    }
}
