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
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.configuration.schemas.table.ColumnChange;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.ITUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Ignition interface tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
abstract class AbstractSchemaChangeTest {
    /** Table name. */
    public static final String TABLE = "PUBLIC.tbl1";

    /** Network ports of the test nodes. */
    private static final int[] PORTS = { 3344, 3345, 3346 };

    /** Nodes bootstrap configuration. */
    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    /** Cluster nodes. */
    private final List<Ignite> clusterNodes = new ArrayList<>();

    /** Work directory */
    @WorkDirectory
    private Path workDir;

    /** */
    @BeforeEach
    void setUp(TestInfo testInfo) {
        String node0Name = testNodeName(testInfo, PORTS[0]);
        String node1Name = testNodeName(testInfo, PORTS[1]);
        String node2Name = testNodeName(testInfo, PORTS[2]);

        nodesBootstrapCfg.put(
            node0Name,
            "{\n" +
            "  node.metastorageNodes: [ \"" + node0Name + "\" ],\n" +
            "  network: {\n" +
            "    port: " + PORTS[0] + ",\n" +
            "    nodeFinder: {\n" +
            "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}"
        );

        nodesBootstrapCfg.put(
            node1Name,
            "{\n" +
            "  node.metastorageNodes: [ \"" + node0Name + "\" ],\n" +
            "  network: {\n" +
            "    port: " + PORTS[1] + ",\n" +
            "    nodeFinder: {\n" +
            "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}"
        );

        nodesBootstrapCfg.put(
            node2Name,
            "{\n" +
            "  node.metastorageNodes: [ \"" + node0Name + "\" ],\n" +
            "  network: {\n" +
            "    port: " + PORTS[2] + ",\n" +
            "    nodeFinder: {\n" +
            "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}"
        );
    }

    /** */
    @AfterEach
    void afterEach() throws Exception {
        IgniteUtils.closeAll(ITUtils.reverse(clusterNodes));
    }

    /**
     * Check unsupported column type change.
     */
    @Test
    public void testChangeColumnType() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        assertColumnChangeFailed(grid, "valStr", c -> c.changeType(t -> t.changeType("UNKNOWN_TYPE")));

        assertColumnChangeFailed(grid, "valInt", colChanger -> colChanger.changeType(t -> t.changeType(ColumnType.blobOf().typeSpec().name())));

        assertColumnChangeFailed(grid, "valInt", colChanger -> colChanger.changeType(t -> t.changePrecision(10)));
        assertColumnChangeFailed(grid, "valInt", colChanger -> colChanger.changeType(t -> t.changeScale(10)));
        assertColumnChangeFailed(grid, "valInt", colChanger -> colChanger.changeType(t -> t.changeLength(1)));

        assertColumnChangeFailed(grid, "valBigInt", colChanger -> colChanger.changeType(t -> t.changePrecision(-1)));
        assertColumnChangeFailed(grid, "valBigInt", colChanger -> colChanger.changeType(t -> t.changePrecision(10)));
        assertColumnChangeFailed(grid, "valBigInt", colChanger -> colChanger.changeType(t -> t.changeScale(2)));
        assertColumnChangeFailed(grid, "valBigInt", colChanger -> colChanger.changeType(t -> t.changeLength(10)));

        assertColumnChangeFailed(grid, "valDecimal", colChanger -> colChanger.changeType(c -> c.changePrecision(-1)));
        assertColumnChangeFailed(grid, "valDecimal", colChanger -> colChanger.changeType(c -> c.changePrecision(0)));
        assertColumnChangeFailed(grid, "valDecimal", colChanger -> colChanger.changeType(c -> c.changeScale(-2)));
        assertColumnChangeFailed(grid, "valDecimal", colChanger -> colChanger.changeType(c -> c.changePrecision(10)));
        assertColumnChangeFailed(grid, "valDecimal", colChanger -> colChanger.changeType(c -> c.changeScale(2)));
        assertColumnChangeFailed(grid, "valDecimal", colChanger -> colChanger.changeType(c -> c.changeLength(10)));
    }

    /**
     * Check unsupported nullability change.
     */
    @Test
    public void testChangeColumnsNullability() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        assertColumnChangeFailed(grid, "valStr", colChanger -> colChanger.changeNullable(true));
        assertColumnChangeFailed(grid, "valInt", colChanger -> colChanger.changeNullable(false));
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
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
            SchemaBuilders.column("key", ColumnType.INT64).asNonNull().build(),
            SchemaBuilders.column("valInt", ColumnType.INT32).asNullable().build(),
            SchemaBuilders.column("valBlob", ColumnType.blobOf()).asNullable().build(),
            SchemaBuilders.column("valDecimal", ColumnType.decimalOf()).asNullable().build(),
            SchemaBuilders.column("valBigInt", ColumnType.numberOf()).asNullable().build(),
            SchemaBuilders.column("valStr", ColumnType.string()).withDefaultValueExpression("default").build()
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
    protected void addColumn(List<Ignite> nodes, ColumnDefinition columnToAdd) {
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

    /**
     * Ensure configuration validation failed.
     *
     * @param grid Grid.
     * @param colName Column to change.
     * @param colChanger Column configuration changer.
     */
    private void assertColumnChangeFailed(List<Ignite> grid, String colName, Consumer<ColumnChange> colChanger) {
        Assertions.assertThrows(ConfigurationValidationException.class, () -> {
            grid.get(0).tables().alterTable(TABLE,
                tblChanger -> tblChanger.changeColumns(cols -> {
                    final String colKey = tblChanger.columns().namedListKeys().stream()
                        .filter(c -> colName.equals(tblChanger.columns().get(c).name()))
                        .findFirst()
                        .orElseGet(() -> Assertions.fail("Column not found."));

                    tblChanger.changeColumns(listChanger -> listChanger.createOrUpdate(colKey, colChanger)
                    );
                })
            );
        });
    }

    /**
     * @param expectedType Expected cause type.
     * @param executable Executable that throws exception.
     */
    public <T extends Throwable> void assertThrowsWithCause(Class<T> expectedType, Executable executable) {
        Throwable ex = assertThrows(IgniteException.class, executable);

        while (ex.getCause() != null) {
            if (expectedType.isInstance(ex.getCause()))
                return;

            ex = ex.getCause();
        }

        fail("Expected cause wasn't found.");
    }
}
