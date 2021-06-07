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

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.storage.ConfigurationType;
import org.apache.ignite.internal.affinity.AffinityManager;
import org.apache.ignite.internal.affinity.event.AffinityEvent;
import org.apache.ignite.internal.affinity.event.AffinityEventParameters;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.internal.metastorage.client.Condition;
import org.apache.ignite.internal.metastorage.client.Operation;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ReflectionUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests scenarios for table manager.
 */
public class TableManagerTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(TableManagerTest.class);

    /** Internal prefix for the metasorage. */
    private static final String INTERNAL_PREFIX = "internal.tables.";

    /** The name of the table which is statically configured. */
    private static final String STATIC_TABLE_NAME = "t1";

    /** The name of the table which will be configured dynamically. */
    private static final String DYNAMIC_TABLE_NAME = "t2";

    /** The name of table to drop it. */
    private static final String DYNAMIC_TABLE_FOR_DROP_NAME = "t3";

    /** Table partitions. */
    public static final int PARTITIONS = 32;

    /** Node port. */
    public static final int PORT = 2245;

    /** Node name. */
    public static final String NODE_NAME = "node1";

    /** Configuration manager. */
    private ConfigurationManager cfrMgr;

    /** Before all test scenarios. */
    @BeforeEach
    private void before() {
        try {
            cfrMgr = new ConfigurationManager(rootConfigurationKeys(), Arrays.asList(
                new TestConfigurationStorage(ConfigurationType.LOCAL),
                new TestConfigurationStorage(ConfigurationType.DISTRIBUTED)));

            cfrMgr.bootstrap("{\n" +
                "   \"node\":{\n" +
                "      \"metastorageNodes\":[\n" +
                "         \"" + NODE_NAME + "\"\n" +
                "      ]\n" +
                "   }\n" +
                "}", ConfigurationType.LOCAL);

            cfrMgr.bootstrap("{\n" +
                "   \"cluster\":{\n" +
                "   \"metastorageNodes\":[\n" +
                "      \"" + NODE_NAME + "\"\n" +
                "   ]\n" +
                "},\n" +
                "   \"table\":{\n" +
                "      \"tables\":{\n" +
                "         \"" + STATIC_TABLE_NAME + "\":{\n" +
                "            \"name\":\"TestTable\",\n" +
                "            \"partitions\":16,\n" +
                "            \"replicas\":1,\n" +
                "            \"columns\":{\n" +
                "               \"id\":{\n" +
                "                  \"name\":\"id\",\n" +
                "                  \"type\":{\n" +
                "                     \"type\":\"Int64\"\n" +
                "                  },\n" +
                "                  \"nullable\":false\n" +
                "               }\n" +
                "            },\n" +
                "            \"indices\":{\n" +
                "               \"pk\":{\n" +
                "                  \"name\":\"pk\",\n" +
                "                  \"type\":\"primary\",\n" +
                "                  \"uniq\":true,\n" +
                "                  \"columns\":{\n" +
                "                     \"id\":{\n" +
                "                        \"name\":\"id\",\n" +
                "                        \"asc\":true\n" +
                "                     }\n" +
                "                  }\n" +
                "               }\n" +
                "            }\n" +
                "         }\n" +
                "      }\n" +
                "   }\n" +
                "}", ConfigurationType.DISTRIBUTED);
        }
        catch (Exception e) {
            LOG.error("Failed to bootstrap the test configuration manager.", e);

            fail("Failed to configure manager [err=" + e.getMessage() + ']');
        }
    }

    /**
     * Tests a table which was defined before start through bootstrap configuration.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-14578")
    @Test
    public void testStaticTableConfigured() {
        MetaStorageManager mm = mock(MetaStorageManager.class);
        SchemaManager sm = mock(SchemaManager.class);
        AffinityManager am = mock(AffinityManager.class);
        Loza rm = mock(Loza.class);
        VaultManager vm = mock(VaultManager.class);

        TableManager tableManager = new TableManager(cfrMgr, mm, sm, am, rm, vm);

        assertEquals(1, tableManager.tables().size());

        assertNotNull(tableManager.table(STATIC_TABLE_NAME));
    }

    /**
     * Tests create a table through public API.
     */
    @Test
    public void testCreateTable() {
        MetaStorageManager mm = mock(MetaStorageManager.class);
        SchemaManager sm = mock(SchemaManager.class);
        AffinityManager am = mock(AffinityManager.class);
        Loza rm = mock(Loza.class);
        VaultManager vm = mock(VaultManager.class);

        ClusterNode node = new ClusterNode(UUID.randomUUID().toString(), NODE_NAME, "127.0.0.1", PORT);

        CompletableFuture<UUID> tblIdFut = new CompletableFuture<>();

        TableManager tableManager = mockManagersAndCreateTable(DYNAMIC_TABLE_NAME, mm, sm, am, rm, vm, node, tblIdFut);

        assertNotNull(tableManager.table("PUBLIC." + DYNAMIC_TABLE_NAME));
    }

    /**
     * Tests drop a table  through public API.
     */
    @Test
    public void testDropTable() {
        MetaStorageManager mm = mock(MetaStorageManager.class);
        SchemaManager sm = mock(SchemaManager.class);
        AffinityManager am = mock(AffinityManager.class);
        Loza rm = mock(Loza.class);
        VaultManager vm = mock(VaultManager.class);

        ClusterNode node = new ClusterNode(UUID.randomUUID().toString(), NODE_NAME, "127.0.0.1", PORT);

        CompletableFuture<UUID> tblIdFut = new CompletableFuture<>();

        TableManager tableManager = mockManagersAndCreateTable(DYNAMIC_TABLE_FOR_DROP_NAME, mm, sm, am, rm, vm, node, tblIdFut);

        assertNotNull(tableManager.table("PUBLIC." + DYNAMIC_TABLE_FOR_DROP_NAME));

        when(sm.unregisterSchemas(any())).thenReturn(CompletableFuture.completedFuture(true));

        doAnswer(invokation -> {
            BiPredicate<SchemaEventParameters, Exception> schemaInitialized = invokation.getArgument(1);

            assertTrue(tblIdFut.isDone());

            SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);

            CompletableFuture.supplyAsync(() -> schemaInitialized.test(
                new SchemaEventParameters(tblIdFut.join(), schemaRegistry),
                null));

            return null;
        }).when(sm).listen(same(SchemaEvent.DROPPED), any());

        when(am.removeAssignment(any())).thenReturn(CompletableFuture.completedFuture(true));

        doAnswer(invokation -> {
            BiPredicate<AffinityEventParameters, Exception> affinityRemovedDelegate = (BiPredicate)invokation.getArgument(1);

            ArrayList<List<ClusterNode>> assignment = new ArrayList<>(PARTITIONS);

            for (int part = 0; part < PARTITIONS; part++) {
                assignment.add(new ArrayList<ClusterNode>(Collections.singleton(node)));
            }

            assertTrue(tblIdFut.isDone());

            CompletableFuture.supplyAsync(() -> affinityRemovedDelegate.test(
                new AffinityEventParameters(tblIdFut.join(), assignment),
                null));

            return null;
        }).when(am).listen(same(AffinityEvent.REMOVED), any());

        tableManager.dropTable("PUBLIC." + DYNAMIC_TABLE_FOR_DROP_NAME);

        assertNull(tableManager.table("PUBLIC." + DYNAMIC_TABLE_FOR_DROP_NAME));
    }

    /**
     * Instantiates Table manager and creates a table in it.
     *
     * @param tableName Table name.
     * @param mm Metastorage manager mock.
     * @param sm Schema manager mock.
     * @param am Affinity manager mock.
     * @param rm Raft manager mock.
     * @param vm Vault manager mock.
     * @param node This cluster node.
     * @param tblIdFut Future which will determine a table id.
     * @return Table manager.
     */
    private TableManager mockManagersAndCreateTable(
        String tableName,
        MetaStorageManager mm,
        SchemaManager sm,
        AffinityManager am,
        Loza rm,
        VaultManager vm,
        ClusterNode node,
        CompletableFuture<UUID> tblIdFut
    ) {
        when(mm.hasMetastorageLocally(any())).thenReturn(true);

        when(mm.invoke((Condition)any(), (Operation)any(), (Operation)any())).thenAnswer(invokation -> {
            Condition condition = (Condition)invokation.getArgument(0);

            Object internalCondition = ReflectionUtils.tryToReadFieldValue(Condition.class, "cond", condition).get();

            Method getKeyMethod = ReflectionUtils.findMethod(internalCondition.getClass(), "key").get();

            byte[] metastorageKeyBytes = (byte[])ReflectionUtils.invokeMethod(getKeyMethod, internalCondition);

            tblIdFut.complete(UUID.fromString(new String(metastorageKeyBytes, StandardCharsets.UTF_8)
                .substring(INTERNAL_PREFIX.length())));

            return CompletableFuture.completedFuture(true);
        });

        when(sm.initSchemaForTable(any(), eq(tableName))).thenReturn(CompletableFuture.completedFuture(true));

        doAnswer(invokation -> {
            BiPredicate<SchemaEventParameters, Exception> schemaInitialized = invokation.getArgument(1);

            assertTrue(tblIdFut.isDone());

            SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);

            CompletableFuture.supplyAsync(() -> schemaInitialized.test(
                new SchemaEventParameters(tblIdFut.join(), schemaRegistry),
                null));

            return null;
        }).when(sm).listen(same(SchemaEvent.INITIALIZED), any());

        when(am.calculateAssignments(any(), eq(tableName))).thenReturn(CompletableFuture.completedFuture(true));

        doAnswer(invokation -> {
            BiPredicate<AffinityEventParameters, Exception> affinityClaculatedDelegate = (BiPredicate)invokation.getArgument(1);

            ArrayList<List<ClusterNode>> assignment = new ArrayList<>(PARTITIONS);

            for (int part = 0; part < PARTITIONS; part++) {
                assignment.add(new ArrayList<ClusterNode>(Collections.singleton(node)));
            }

            assertTrue(tblIdFut.isDone());

            CompletableFuture.supplyAsync(() -> affinityClaculatedDelegate.test(
                new AffinityEventParameters(tblIdFut.join(), assignment),
                null));

            return null;
        }).when(am).listen(same(AffinityEvent.CALCULATED), any());

        TableManager tableManager = new TableManager(cfrMgr, mm, sm, am, rm, vm);

        int tablesBeforeCreation = tableManager.tables().size();

        SchemaTable scmTbl2 = SchemaBuilders.tableBuilder("PUBLIC", tableName).columns(
            SchemaBuilders.column("key", ColumnType.INT64).asNonNull().build(),
            SchemaBuilders.column("val", ColumnType.INT64).asNullable().build()
        ).withPrimaryKey("key").build();

        Table tbl2 = tableManager.createTable(scmTbl2.canonicalName(), tblCh -> SchemaConfigurationConverter.convert(scmTbl2, tblCh)
            .changeReplicas(1)
            .changePartitions(10)
        );

        assertNotNull(tbl2);

        assertEquals(tablesBeforeCreation + 1, tableManager.tables().size());

        return tableManager;
    }

    /**
     * Gets a list of configuration keys to use in the test scenario.
     *
     * @return List of root configuration keys.
     */
    private static List<RootKey<?, ?>> rootConfigurationKeys() {
        return Arrays.asList(
            NodeConfiguration.KEY,
            ClusterConfiguration.KEY,
            TablesConfiguration.KEY
        );
    }
}
