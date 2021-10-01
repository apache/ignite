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

package org.apache.ignite.internal.table;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfigurationSchema;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Tests scenarios for table manager.
 */
@ExtendWith({MockitoExtension.class, WorkDirectoryExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class TableManagerTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(TableManagerTest.class);

    /** Public prefix for metastorage. */
    private static final String PUBLIC_PREFIX = "dst-cfg.table.tables.";

    /** The name of the table which is statically configured. */
    private static final String STATIC_TABLE_NAME = "t1";

    /** The name of the table which will be configured dynamically. */
    private static final String DYNAMIC_TABLE_NAME = "t2";

    /** The name of table to drop it. */
    private static final String DYNAMIC_TABLE_FOR_DROP_NAME = "t3";

    /** Table partitions. */
    private static final int PARTITIONS = 32;

    /** Node name. */
    private static final String NODE_NAME = "node1";

    /** Node configuration manager. */
    private ConfigurationManager nodeCfgMgr;

    /** Cluster configuration manager. */
    private ConfigurationManager clusterCfgMgr;

    /** MetaStorage manager. */
    @Mock(lenient = true)
    private MetaStorageManager mm;

    /** Schema manager. */
    @Mock(lenient = true)
    private BaselineManager bm;

    /** Topology service. */
    @Mock(lenient = true)
    private TopologyService ts;

    /** Raft manager. */
    @Mock(lenient = true)
    private Loza rm;

    @WorkDirectory
    private Path workDir;

    /** Test node. */
    private final ClusterNode node = new ClusterNode(
        UUID.randomUUID().toString(),
        NODE_NAME,
        new NetworkAddress("127.0.0.1", 2245)
    );

    /** Before all test scenarios. */
    @BeforeEach
    void setUp() {
        try {
            nodeCfgMgr = new ConfigurationManager(
                List.of(NodeConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(LOCAL),
                List.of()
            );

            clusterCfgMgr = new ConfigurationManager(
                List.of(ClusterConfiguration.KEY, TablesConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                Collections.singletonList(ExtendedTableConfigurationSchema.class)
            );

            nodeCfgMgr.start();
            clusterCfgMgr.start();

            nodeCfgMgr.bootstrap("{\n" +
                "   \"node\":{\n" +
                "      \"metastorageNodes\":[\n" +
                "         \"" + NODE_NAME + "\"\n" +
                "      ]\n" +
                "   }\n" +
                "}");
        }
        catch (Exception e) {
            LOG.error("Failed to bootstrap the test configuration manager.", e);

            fail("Failed to configure manager [err=" + e.getMessage() + ']');
        }
    }

    /** Stop configuration manager. */
    @AfterEach
    void tearDown() {
        nodeCfgMgr.stop();
        clusterCfgMgr.stop();
    }

    /**
     * Tests a table which was defined before start through bootstrap configuration.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15255")
    @Test
    public void testStaticTableConfigured() {
        TableManager tableManager = new TableManager(
            clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY),
            rm,
            bm,
            ts,
            mm,
            workDir
        );

        assertEquals(1, tableManager.tables().size());

        assertNotNull(tableManager.table(STATIC_TABLE_NAME));
    }

    /**
     * Tests create a table through public API.
     */
    @Test
    public void testCreateTable() {
        CompletableFuture<TableManager> tblManagerFut = new CompletableFuture<>();

        TableDefinition scmTbl = SchemaBuilders.tableBuilder("PUBLIC", DYNAMIC_TABLE_NAME).columns(
            SchemaBuilders.column("key", ColumnType.INT64).asNonNull().build(),
            SchemaBuilders.column("val", ColumnType.INT64).asNullable().build()
        ).withPrimaryKey("key").build();

        Table table = mockManagersAndCreateTable(scmTbl, tblManagerFut);

        assertNotNull(table);

        assertSame(table, tblManagerFut.join().table(scmTbl.canonicalName()));
    }

    /**
     * Tests drop a table through public API.
     */
    @Test
    public void testDropTable() {
        CompletableFuture<TableManager> tblManagerFut = new CompletableFuture<>();

        TableDefinition scmTbl = SchemaBuilders.tableBuilder("PUBLIC", DYNAMIC_TABLE_FOR_DROP_NAME).columns(
            SchemaBuilders.column("key", ColumnType.INT64).asNonNull().build(),
            SchemaBuilders.column("val", ColumnType.INT64).asNullable().build()
        ).withPrimaryKey("key").build();

        mockManagersAndCreateTable(scmTbl, tblManagerFut);

        TableManager tableManager = tblManagerFut.join();

        tableManager.dropTable(scmTbl.canonicalName());

        assertNull(tableManager.table(scmTbl.canonicalName()));

        assertEquals(0, tableManager.tables().size());
    }

    /**
     * Instantiates a table and prepares Table manager.
     */
    @Test
    public void testGetTableDuringCreation() throws Exception {
        CompletableFuture<TableManager> tblManagerFut = new CompletableFuture<>();

        TableDefinition scmTbl = SchemaBuilders.tableBuilder("PUBLIC", DYNAMIC_TABLE_FOR_DROP_NAME).columns(
            SchemaBuilders.column("key", ColumnType.INT64).asNonNull().build(),
            SchemaBuilders.column("val", ColumnType.INT64).asNullable().build()
        ).withPrimaryKey("key").build();

        Phaser phaser = new Phaser(2);

        CompletableFuture<Table> createFut = CompletableFuture.supplyAsync(() ->
            mockManagersAndCreateTableWithDelay(scmTbl, tblManagerFut, phaser)
        );

        CompletableFuture<Table> getFut = CompletableFuture.supplyAsync(() -> {
            phaser.awaitAdvance(0);

            return tblManagerFut.join().table(scmTbl.canonicalName());
        });

        CompletableFuture<Collection<Table>> getAllTablesFut = CompletableFuture.supplyAsync(() -> {
            phaser.awaitAdvance(0);

            return tblManagerFut.join().tables();
        });

        assertFalse(createFut.isDone());
        assertFalse(getFut.isDone());
        assertFalse(getAllTablesFut.isDone());

        phaser.arrive();

        assertSame(createFut.join(), getFut.join());

        assertEquals(1, getAllTablesFut.join().size());
    }

    /**
     * Tries to create a table that already exists.
     */
    @Test
    public void testDoubledCreateTable() {
        CompletableFuture<TableManager> tblManagerFut = new CompletableFuture<>();

        TableDefinition scmTbl = SchemaBuilders.tableBuilder("PUBLIC", DYNAMIC_TABLE_NAME)
            .columns(
                SchemaBuilders.column("key", ColumnType.INT64).asNonNull().build(),
                SchemaBuilders.column("val", ColumnType.INT64).asNullable().build())
            .withPrimaryKey("key")
            .build();

        Table table = mockManagersAndCreateTable(scmTbl, tblManagerFut);

        assertNotNull(table);

        assertThrows(RuntimeException.class, () -> tblManagerFut.join().createTable(scmTbl.canonicalName(), tblCh -> SchemaConfigurationConverter.convert(scmTbl, tblCh)
            .changeReplicas(1)
            .changePartitions(10)));

        assertSame(table, tblManagerFut.join().createTableIfNotExists(scmTbl.canonicalName(), tblCh -> SchemaConfigurationConverter.convert(scmTbl, tblCh)
            .changeReplicas(1)
            .changePartitions(10)));
    }

    /**
     * Instantiates Table manager and creates a table in it.
     *
     * @param tableDefinition Configuration schema for a table.
     * @param tblManagerFut Future for table manager.
     * @return Table.
     */
    private TableImpl mockManagersAndCreateTable(
        TableDefinition tableDefinition,
        CompletableFuture<TableManager> tblManagerFut
    ) {
        return mockManagersAndCreateTableWithDelay(tableDefinition, tblManagerFut, null);
    }

    /**
     * Instantiates a table and prepares Table manager. When the latch would open, the method completes.
     *
     * @param tableDefinition Configuration schema for a table.
     * @param tblManagerFut Future for table manager.
     * @param phaser Phaser for the wait.
     * @return Table manager.
     */
    @NotNull private TableImpl mockManagersAndCreateTableWithDelay(
        TableDefinition tableDefinition,
        CompletableFuture<TableManager> tblManagerFut,
        Phaser phaser
    ) {
        when(rm.prepareRaftGroup(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        AtomicBoolean tableCreatedFlag = new AtomicBoolean();

        try (MockedStatic<SchemaUtils> schemaServiceMock = mockStatic(SchemaUtils.class)) {
            schemaServiceMock.when(() -> SchemaUtils.prepareSchemaDescriptor(anyInt(), any())).
                thenReturn(mock(SchemaDescriptor.class));
        }

        try (MockedStatic<AffinityUtils> affinityServiceMock = mockStatic(AffinityUtils.class)) {
            ArrayList<List<ClusterNode>> assignment = new ArrayList<>(PARTITIONS);

            for (int part = 0; part < PARTITIONS; part++)
                assignment.add(new ArrayList<>(Collections.singleton(node)));

            affinityServiceMock.when(() -> AffinityUtils.calculateAssignments(any(), anyInt(), anyInt())).
                thenReturn(assignment);
        }

        TableManager tableManager = new TableManager(
            clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY),
            rm,
            bm,
            ts,
            mm,
            workDir
        );

        TableImpl tbl2 = null;

        try {
            tableManager.start();

            tblManagerFut.complete(tableManager);

            when(mm.range(eq(new ByteArray(PUBLIC_PREFIX)), any())).thenAnswer(invocation -> {
                Cursor<Entry> cursor = mock(Cursor.class);

                when(cursor.hasNext()).thenReturn(false);

                return cursor;
            });

            int tablesBeforeCreation = tableManager.tables().size();

            clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().listen(ctx -> {
                boolean createTbl = ctx.newValue().get(tableDefinition.canonicalName()) != null &&
                    ctx.oldValue().get(tableDefinition.canonicalName()) == null;

                boolean dropTbl = ctx.oldValue().get(tableDefinition.canonicalName()) != null &&
                    ctx.newValue().get(tableDefinition.canonicalName()) == null;

                if (!createTbl && !dropTbl)
                    return CompletableFuture.completedFuture(null);

                tableCreatedFlag.set(createTbl);

                try {
                    when(mm.range(eq(new ByteArray(PUBLIC_PREFIX)), any())).thenAnswer(invocation -> {
                        AtomicBoolean firstRecord = new AtomicBoolean(createTbl);

                        Cursor<Entry> cursor = mock(Cursor.class);

                        when(cursor.hasNext()).thenAnswer(hasNextInvocation ->
                            firstRecord.compareAndSet(true, false));

                        Entry mockEntry = mock(Entry.class);

                        when(mockEntry.key()).thenReturn(new ByteArray(PUBLIC_PREFIX + "uuid." + NamedListNode.NAME));

                        when(mockEntry.value()).thenReturn(ByteUtils.toBytes(tableDefinition.canonicalName()));

                        when(cursor.next()).thenReturn(mockEntry);

                        return cursor;
                    });
                }
                catch (NodeStoppingException e) {
                    LOG.error("Node was stopped during table creation.", e);

                    fail();
                }

                if (phaser != null)
                    phaser.arriveAndAwaitAdvance();

                return CompletableFuture.completedFuture(null);
            });

            tbl2 = (TableImpl)tableManager.createTable(tableDefinition.canonicalName(), tblCh -> SchemaConfigurationConverter.convert(tableDefinition, tblCh)
                .changeReplicas(1)
                .changePartitions(10)
            );

            assertNotNull(tbl2);

            assertEquals(tablesBeforeCreation + 1, tableManager.tables().size());
        }
        catch (NodeStoppingException e) {
            LOG.error("Node was stopped during table creation.", e);

            fail();
        }
        finally {
            tableManager.stop();
        }

        return tbl2;
    }
}
