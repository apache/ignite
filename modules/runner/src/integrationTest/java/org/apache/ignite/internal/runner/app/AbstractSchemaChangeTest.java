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

package org.apache.ignite.internal.runner.app;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import com.google.common.collect.Lists;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;

/**
 * Ignition interface tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
abstract class AbstractSchemaChangeTest {
    /** Table name. */
    public static final String TABLE = "PUBLIC.tbl1";

    /** Nodes bootstrap configuration. */
    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>() {{
        put("node0", "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ \"node0\" ]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3344,\n" +
            "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "  }\n" +
            "}");

        put("node1", "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ \"node0\" ]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3345,\n" +
            "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "  }\n" +
            "}");

        put("node2", "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ \"node0\" ]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3346,\n" +
            "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "  }\n" +
            "}");
    }};

    /** Cluster nodes. */
    private final List<Ignite> clusterNodes = new ArrayList<>();

    /** Work directory */
    @WorkDirectory
    private Path workDir;

    /**
     *
     */
    @AfterEach
    void afterEach() throws Exception {
        IgniteUtils.closeAll(Lists.reverse(clusterNodes));
    }

    /**
     * Check unsupported column type change.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15056")
    @Test
    public void testChangeColumnType() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Assertions.assertThrows(InvalidTypeException.class, () -> {
            grid.get(0).tables().alterTable(TABLE,
                tblChanger -> tblChanger.changeColumns(cols -> {
                    final String colKey = tblChanger.columns().namedListKeys().stream()
                        .filter(c -> "valInt".equals(tblChanger.columns().get(c).name()))
                        .findFirst()
                        .orElseThrow(() -> {
                            throw new IllegalStateException("Column not found.");
                        });

                    tblChanger.changeColumns(listChanger ->
                        listChanger.createOrUpdate(colKey, colChanger -> colChanger.changeType(c -> c.changeType("STRING")))
                    );
                })
            );
        });
    }

    /**
     * Check unsupported column nullability change.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15056")
    @Test
    public void testMakeColumnNonNullable() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Assertions.assertThrows(InvalidTypeException.class, () -> {
            grid.get(0).tables().alterTable(TABLE,
                tblChanger -> tblChanger.changeColumns(cols -> {
                    final String colKey = tblChanger.columns().namedListKeys().stream()
                        .filter(c -> "valInt".equals(tblChanger.columns().get(c).name()))
                        .findFirst()
                        .orElseThrow(() -> {
                            throw new IllegalStateException("Column not found.");
                        });

                    tblChanger.changeColumns(listChanger ->
                        listChanger.createOrUpdate(colKey, colChanger -> colChanger.changeNullable(false))
                    );
                })
            );
        });
    }

    /**
     * Check unsupported nullability change.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15056")
    @Test
    public void testMakeColumnsNullable() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Assertions.assertThrows(InvalidTypeException.class, () -> {
            grid.get(0).tables().alterTable(TABLE,
                tblChanger -> tblChanger.changeColumns(cols -> {
                    final String colKey = tblChanger.columns().namedListKeys().stream()
                        .filter(c -> "valStr".equals(tblChanger.columns().get(c).name()))
                        .findFirst()
                        .orElseThrow(() -> {
                            throw new IllegalStateException("Column not found.");
                        });

                    tblChanger.changeColumns(listChanger ->
                        listChanger.createOrUpdate(colKey, colChanger -> colChanger.changeNullable(true))
                    );
                })
            );
        });
    }

    /**
     * @return Grid nodes.
     */
    @NotNull protected List<Ignite> startGrid() {
        nodesBootstrapCfg.forEach((nodeName, configStr) ->
            clusterNodes.add(IgnitionManager.start(nodeName, configStr, workDir.resolve(nodeName)))
        );

        return clusterNodes;
    }

    /**
     * @param nodes Cluster nodes.
     */
    protected void createTable(List<Ignite> nodes) {
        // Create table on node 0.
        SchemaTable schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
            SchemaBuilders.column("key", ColumnType.INT64).asNonNull().build(),
            SchemaBuilders.column("valInt", ColumnType.INT32).asNullable().build(),
            SchemaBuilders.column("valStr", ColumnType.string()).withDefaultValue("default").build()
        ).withPrimaryKey("key").build();

        nodes.get(0).tables().createTable(
            schTbl1.canonicalName(),
            tblCh -> convert(schTbl1, tblCh).changeReplicas(1).changePartitions(10)
        );
    }

    /**
     * @param nodes Cluster nodes.
     * @param columnToAdd Column to add.
     */
    protected void addColumn(List<Ignite> nodes, Column columnToAdd) {
        nodes.get(0).tables().alterTable(TABLE,
            chng -> chng.changeColumns(cols -> {
                int colIdx = chng.columns().namedListKeys().stream().mapToInt(Integer::parseInt).max().getAsInt() + 1;

                cols.create(String.valueOf(colIdx), colChg -> convert(columnToAdd, colChg));
            })
        );
    }

    /**
     * @param nodes Cluster nodes.
     * @param colName Name of column to drop.
     */
    protected void dropColumn(List<Ignite> nodes, String colName) {
        nodes.get(0).tables().alterTable(TABLE,
            chng -> chng.changeColumns(cols -> {
                cols.delete(chng.columns().namedListKeys().stream()
                    .filter(key -> colName.equals(chng.columns().get(key).name()))
                    .findAny()
                    .orElseThrow(() -> {
                        throw new IllegalStateException("Column not found.");
                    })
                );
            })
        );
    }

    /**
     * @param nodes Cluster nodes.
     * @param oldName Old column name.
     * @param newName New column name.
     */
    protected void renameColumn(List<Ignite> nodes, String oldName, String newName) {
        nodes.get(0).tables().alterTable(TABLE,
            tblChanger -> tblChanger.changeColumns(cols -> {
                final String colKey = tblChanger.columns().namedListKeys().stream()
                    .filter(c -> oldName.equals(tblChanger.columns().get(c).name()))
                    .findFirst()
                    .orElseThrow(() -> {
                        throw new IllegalStateException("Column not found.");
                    });

                tblChanger.changeColumns(listChanger ->
                    listChanger.createOrUpdate(colKey, colChanger -> colChanger.changeName(newName))
                );
            })
        );
    }

    /**
     * @param nodes Cluster nodes.
     * @param colName Column name.
     * @param defSup Default value supplier.
     */
    protected void changeDefault(List<Ignite> nodes, String colName, Supplier<Object> defSup) {
        nodes.get(0).tables().alterTable(TABLE,
            tblChanger -> tblChanger.changeColumns(cols -> {
                final String colKey = tblChanger.columns().namedListKeys().stream()
                    .filter(c -> colName.equals(tblChanger.columns().get(c).name()))
                    .findFirst()
                    .orElseThrow(() -> {
                        throw new IllegalStateException("Column not found.");
                    });

                tblChanger.changeColumns(listChanger ->
                    listChanger.createOrUpdate(colKey, colChanger -> colChanger.changeDefaultValue(defSup.get().toString()))
                );
            })
        );
    }
}
