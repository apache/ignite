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

package org.apache.ignite.internal.sql.engine.exec;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.store.RocksDbDataRegionConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.PartialIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfigurationSchema;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
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

/** Mock ddl usage. */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class DdlWithMockedManagersTest extends IgniteAbstractTest {
    /** Node name. */
    private static final String NODE_NAME = "node1";

    /** Schema manager. */
    @Mock
    private BaselineManager bm;

    /** Topology service. */
    @Mock
    private TopologyService ts;

    /** Cluster service. */
    @Mock(lenient = true)
    private ClusterService cs;

    /** Raft manager. */
    @Mock
    private Loza rm;

    /** TX manager. */
    @Mock(lenient = true)
    private TxManager tm;

    /** Tables configuration. */
    @InjectConfiguration(
            internalExtensions = ExtendedTableConfigurationSchema.class,
            polymorphicExtensions = {
                    HashIndexConfigurationSchema.class, SortedIndexConfigurationSchema.class, PartialIndexConfigurationSchema.class
            }
    )

    private TablesConfiguration tblsCfg;

    /** Data storage configuration. */
    @InjectConfiguration(polymorphicExtensions = RocksDbDataRegionConfigurationSchema.class)
    private DataStorageConfiguration dataStorageCfg;

    TableManager tblManager;

    SqlQueryProcessor queryProc;

    /** Test node. */
    private final ClusterNode node = new ClusterNode(
            UUID.randomUUID().toString(),
            NODE_NAME,
            new NetworkAddress("127.0.0.1", 2245)
    );

    /** Returns current method name. */
    private static String getCurrentMethodName() {
        return StackWalker.getInstance()
                .walk(s -> s.skip(1).findFirst())
                .get()
                .getMethodName();
    }

    /** Stop configuration manager. */
    @AfterEach
    void after() {
        try {
            Objects.requireNonNull(queryProc).stop();
        } catch (Exception e) {
            fail(e);
        }

        Objects.requireNonNull(tblManager).stop();
    }

    /** Inner initialisation. */
    @BeforeEach
    void before() throws NodeStoppingException {
        tblManager = mockManagers();

        queryProc = new SqlQueryProcessor(cs, tblManager);

        queryProc.start();
    }

    /**
     * Tests create a table through public API.
     */
    @Test
    public void testCreateTable() throws Exception {
        SqlQueryProcessor finalQueryProc = queryProc;

        String curMethodName = getCurrentMethodName();

        String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) "
                + "with partitions=1,replicas=1", curMethodName);

        queryProc.query("PUBLIC", newTblSql);

        assertTrue(tblManager.tables().stream().anyMatch(t -> t.name()
                .equalsIgnoreCase("PUBLIC." + curMethodName)));

        String finalNewTblSql1 = newTblSql;

        assertThrows(TableAlreadyExistsException.class, () -> finalQueryProc.query("PUBLIC", finalNewTblSql1));

        String finalNewTblSql2 = String.format("CREATE TABLE \"PUBLIC\".%s (c1 int PRIMARY KEY, c2 varbinary(255)) "
                + "with partitions=1,replicas=1", curMethodName);

        assertThrows(TableAlreadyExistsException.class, () -> finalQueryProc.query("PUBLIC", finalNewTblSql2));

        // todo: correct exception need to be thrown https://issues.apache.org/jira/browse/IGNITE-16084
        assertThrows(IgniteInternalException.class, () -> finalQueryProc.query("PUBLIC",
                "CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with partitions__wrong=1,replicas=1"));

        assertThrows(IgniteInternalException.class, () -> finalQueryProc.query("PUBLIC",
                "CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with partitions=1,replicas__wrong=1"));

        newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varchar(255))",
                " IF NOT EXISTS " + curMethodName);

        String finalNewTblSql3 = newTblSql;

        assertDoesNotThrow(() -> finalQueryProc.query("PUBLIC", finalNewTblSql3));
    }

    /**
     * Tests create a table with multiple pk through public API.
     */
    @Test
    public void testCreateTableMultiplePk() throws Exception {
        String curMethodName = getCurrentMethodName();

        String newTblSql = String.format("CREATE TABLE %s (c1 int, c2 int NOT NULL DEFAULT 1, c3 int, primary key(c1, c2))", curMethodName);

        queryProc.query("PUBLIC", newTblSql);

        assertTrue(tblManager.tables().stream().anyMatch(t -> t.name()
                .equalsIgnoreCase("PUBLIC." + curMethodName)));
    }

    /**
     * Tests create and drop table through public API.
     */
    @Test
    public void testDropTable() throws Exception {
        String curMethodName = getCurrentMethodName();

        String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varchar(255))", curMethodName);

        queryProc.query("PUBLIC", newTblSql);

        queryProc.query("PUBLIC", "DROP TABLE " + curMethodName);

        SqlQueryProcessor finalQueryProc = queryProc;

        assertThrows(TableNotFoundException.class, () -> finalQueryProc.query("PUBLIC",
                "DROP TABLE " + curMethodName + "_not_exist"));

        assertThrows(TableNotFoundException.class, () -> finalQueryProc.query("PUBLIC",
                "DROP TABLE " + curMethodName));

        assertThrows(TableNotFoundException.class, () -> finalQueryProc.query("PUBLIC",
                "DROP TABLE PUBLIC." + curMethodName));

        queryProc.query("PUBLIC", "DROP TABLE IF EXISTS PUBLIC." + curMethodName + "_not_exist");

        queryProc.query("PUBLIC", "DROP TABLE IF EXISTS PUBLIC." + curMethodName);

        assertTrue(tblManager.tables().stream().noneMatch(t -> t.name()
                .equalsIgnoreCase("PUBLIC." + curMethodName)));
    }

    /**
     * Tests alter and drop columns through public API.
     */
    @Test
    public void testAlterAndDropSimpleCase() throws Exception {
        SqlQueryProcessor finalQueryProc = queryProc;

        String curMethodName = getCurrentMethodName();

        String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varchar(255))", curMethodName);

        queryProc.query("PUBLIC", newTblSql);

        String alterCmd = String.format("ALTER TABLE %s ADD COLUMN (c3 varchar, c4 int)", curMethodName);

        queryProc.query("PUBLIC", alterCmd);

        String alterCmd1 = String.format("ALTER TABLE %s ADD COLUMN c5 int NOT NULL DEFAULT 1", curMethodName);

        queryProc.query("PUBLIC", alterCmd1);

        assertThrows(ColumnAlreadyExistsException.class, () -> finalQueryProc.query("PUBLIC", alterCmd));

        String alterCmdNoTbl = String.format("ALTER TABLE %s ADD COLUMN (c3 varchar, c4 int)", curMethodName + "_notExist");

        assertThrows(TableNotFoundException.class, () -> queryProc.query("PUBLIC", alterCmdNoTbl));

        String alterIfExistsCmd = String.format("ALTER TABLE IF EXISTS %s ADD COLUMN (c3 varchar, c4 int)", curMethodName + "NotExist");

        queryProc.query("PUBLIC", alterIfExistsCmd);

        assertThrows(ColumnAlreadyExistsException.class, () -> finalQueryProc.query("PUBLIC", alterCmd));

        finalQueryProc.query("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN c4", curMethodName));

        queryProc.query("PUBLIC", String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS c3 varchar", curMethodName));

        queryProc.query("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN c3", curMethodName));

        queryProc.query("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN IF EXISTS c3", curMethodName));

        assertThrows(ColumnNotFoundException.class, () -> finalQueryProc.query("PUBLIC",
                String.format("ALTER TABLE %s DROP COLUMN (c3, c4)", curMethodName)));

        assertThrows(IgniteException.class, () -> finalQueryProc.query("PUBLIC",
                String.format("ALTER TABLE %s DROP COLUMN c1", curMethodName)));
    }

    /**
     * Tests alter add multiple columns through public API.
     */
    @Test
    public void testAlterColumnsAddBatch() throws Exception {
        String curMethodName = getCurrentMethodName();

        queryProc.query("PUBLIC", String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varchar(255))", curMethodName));

        queryProc.query("PUBLIC", String.format("ALTER TABLE %s ADD COLUMN (c3 varchar, c4 varchar)", curMethodName));

        queryProc.query("PUBLIC", String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS (c3 varchar, c4 varchar)", curMethodName));

        queryProc.query("PUBLIC", String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS (c3 varchar, c4 varchar, c5 varchar)",
                curMethodName));

        SqlQueryProcessor finalQueryProc = queryProc;

        assertThrows(ColumnAlreadyExistsException.class, () -> finalQueryProc.query("PUBLIC",
                String.format("ALTER TABLE %s ADD COLUMN (c5 varchar)", curMethodName)));
    }

    /**
     * Tests alter drop multiple columns through public API.
     */
    @Test
    public void testAlterColumnsDropBatch() throws Exception {
        String curMethodName = getCurrentMethodName();

        queryProc.query("PUBLIC", String.format("CREATE TABLE %s "
                + "(c1 int PRIMARY KEY, c2 decimal(10), c3 varchar, c4 varchar, c5 varchar)", curMethodName));

        queryProc.query("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN c4", curMethodName));

        queryProc.query("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN IF EXISTS (c3, c4, c5)", curMethodName));

        SqlQueryProcessor finalQueryProc = queryProc;

        assertThrows(ColumnNotFoundException.class, () -> finalQueryProc.query("PUBLIC",
                String.format("ALTER TABLE %s DROP COLUMN c4", curMethodName)));
    }

    /**
     * Tests create a table through public API.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16032")
    @Test
    public void testCreateDropIndex() throws Exception {
        SqlQueryProcessor finalQueryProc = queryProc;

        String curMethodName = getCurrentMethodName();

        String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with partitions=1", curMethodName);

        queryProc.query("PUBLIC", newTblSql);

        assertTrue(tblManager.tables().stream().anyMatch(t -> t.name()
                .equalsIgnoreCase("PUBLIC." + curMethodName)));

        queryProc.query("PUBLIC", String.format("CREATE INDEX index1 ON %s (c1)", curMethodName));

        queryProc.query("PUBLIC", String.format("CREATE INDEX IF NOT EXISTS index1 ON %s (c1)", curMethodName));

        queryProc.query("PUBLIC", String.format("CREATE INDEX index2 ON %s (c1)", curMethodName));

        queryProc.query("PUBLIC", String.format("CREATE INDEX index3 ON %s (c2)", curMethodName));

        assertThrows(IndexAlreadyExistsException.class, () ->
                finalQueryProc.query("PUBLIC", String.format("CREATE INDEX index3 ON %s (c1)", curMethodName)));

        assertThrows(IgniteException.class, () ->
                finalQueryProc.query("PUBLIC", String.format("CREATE INDEX index_3 ON %s (c1)", curMethodName + "_nonExist")));

        queryProc.query("PUBLIC", String.format("CREATE INDEX index4 ON %s (c2 desc, c1 asc)", curMethodName));

        queryProc.query("PUBLIC", String.format("DROP INDEX index4 ON %s", curMethodName));

        queryProc.query("PUBLIC", String.format("CREATE INDEX index4 ON %s (c2 desc, c1 asc)", curMethodName));

        queryProc.query("PUBLIC", String.format("DROP INDEX index4 ON %s", curMethodName));

        queryProc.query("PUBLIC", String.format("DROP INDEX IF EXISTS index4 ON %s", curMethodName));
    }

    // todo copy-paste from TableManagerTest will be removed after https://issues.apache.org/jira/browse/IGNITE-16050
    /**
     * Instantiates a table and prepares Table manager.
     *
     * @return Table manager.
     */
    private TableManager mockManagers() throws NodeStoppingException {
        when(rm.prepareRaftGroup(any(), any(), any())).thenAnswer(mock -> {
            RaftGroupService raftGrpSrvcMock = mock(RaftGroupService.class);

            when(raftGrpSrvcMock.leader()).thenReturn(new Peer(new NetworkAddress("localhost", 47500)));

            return CompletableFuture.completedFuture(raftGrpSrvcMock);
        });

        when(ts.getByAddress(any(NetworkAddress.class))).thenReturn(new ClusterNode(
                UUID.randomUUID().toString(),
                "node0",
                new NetworkAddress("localhost", 47500)
        ));

        try (MockedStatic<SchemaUtils> schemaServiceMock = mockStatic(SchemaUtils.class)) {
            schemaServiceMock.when(() -> SchemaUtils.prepareSchemaDescriptor(anyInt(), any()))
                    .thenReturn(mock(SchemaDescriptor.class));
        }

        when(cs.messagingService()).thenAnswer(invocation -> {
            MessagingService ret = mock(MessagingService.class);

            return ret;
        });

        when(cs.localConfiguration()).thenAnswer(invocation -> {
            ClusterLocalConfiguration ret = mock(ClusterLocalConfiguration.class);

            when(ret.getName()).thenReturn("node1");

            return ret;
        });

        when(cs.topologyService()).thenAnswer(invocation -> {
            TopologyService ret = mock(TopologyService.class);

            when(ret.localMember()).thenReturn(new ClusterNode("1", "node1", null));

            return ret;
        });

        TableManager tableManager = createTableManager();

        return tableManager;
    }

    /**
     * Creates Table manager.
     *
     * @return Table manager.
     */
    @NotNull
    private TableManager createTableManager() {
        TableManager tableManager = new TableManager(
                tblsCfg,
                dataStorageCfg,
                rm,
                bm,
                ts,
                workDir,
                tm
        );

        tableManager.start();

        return tableManager;
    }
}
