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

package org.apache.ignite.internal.calcite;

import static org.apache.ignite.internal.calcite.util.Commons.getAllFromCursor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.ItUtils;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.calcite.util.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.QueryProcessor;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.builder.TableDefinitionBuilder;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract basic integration test.
 */
@ExtendWith(WorkDirectoryExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AbstractBasicIntegrationTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = IgniteLogger.forClass(AbstractBasicIntegrationTest.class);

    /** Base port number. */
    private static final int BASE_PORT = 3344;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"node\": {\n"
            + "    \"metastorageNodes\":[ {} ]\n"
            + "  },\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    /** Cluster nodes. */
    protected static final List<Ignite> CLUSTER_NODES = new ArrayList<>();

    /** Work directory. */
    @WorkDirectory
    private static Path WORK_DIR;

    /**
     * Before all.
     *
     * @param testInfo Test information oject.
     */
    @BeforeAll
    void startNodes(TestInfo testInfo) {
        //TODO: IGNITE-16034 Here we assume that Metastore consists into one node, and it starts at first.
        String metastorageNodes = '\"' + IgniteTestUtils.testNodeName(testInfo, 0) + '\"';

        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        for (int i = 0; i < nodes(); i++) {
            String curNodeName = IgniteTestUtils.testNodeName(testInfo, i);

            CLUSTER_NODES.add(IgnitionManager.start(curNodeName, IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG,
                    metastorageNodes,
                    BASE_PORT + i,
                    connectNodeAddr
            ), WORK_DIR.resolve(curNodeName)));
        }
    }

    /**
     * Get a count of nodes in the Ignite cluster.
     *
     * @return Count of nodes.
     */
    protected int nodes() {
        return 3;
    }

    /**
     * After all.
     */
    @AfterAll
    void stopNodes() throws Exception {
        LOG.info("Start tearDown()");

        IgniteUtils.closeAll(ItUtils.reverse(CLUSTER_NODES));

        CLUSTER_NODES.clear();

        LOG.info("End tearDown()");
    }

    /**
     * Invokes before the test will start.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @BeforeEach
    public void setup(TestInfo testInfo) throws Exception {
        setupBase(testInfo, WORK_DIR);
    }

    /**
     * Invokes after the test has finished.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @AfterEach
    public void tearDown(TestInfo testInfo) throws Exception {
        tearDownBase(testInfo);
    }

    protected static QueryChecker assertQuery(String qry) {
        return new QueryChecker(qry) {
            @Override
            protected QueryProcessor getEngine() {
                return ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine();
            }
        };
    }

    protected static Table createAndPopulateTable() {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "PERSON").columns(
                SchemaBuilders.column("ID", ColumnType.INT32).build(),
                SchemaBuilders.column("NAME", ColumnType.string()).asNullable(true).build(),
                SchemaBuilders.column("SALARY", ColumnType.DOUBLE).asNullable(true).build()
        ).withPrimaryKey("ID").build();

        Table tbl = CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl1, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        );

        int idx = 0;

        insertData(tbl, new String[]{"ID", "NAME", "SALARY"}, new Object[][]{
                {idx++, "Igor", 10d},
                {idx++, null, 15d},
                {idx++, "Ilya", 15d},
                {idx++, "Roma", 10d},
                {idx, "Roma", 10d}
        });

        return tbl;
    }

    protected static void createTable(TableDefinitionBuilder tblBld) {
        TableDefinition schTbl1 = tblBld.build();

        CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl1, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        );
    }

    protected static void insertData(String tblName, String[] columnNames, Object[]... tuples) {
        insertData(CLUSTER_NODES.get(0).tables().table(tblName), columnNames, tuples);
    }

    protected static void insertData(Table table, String[] columnNames, Object[]... tuples) {
        RecordView<Tuple> view = table.recordView();

        int batchSize = 128;

        List<Tuple> batch = new ArrayList<>(batchSize);
        for (Object[] tuple : tuples) {
            assert tuple != null && tuple.length == columnNames.length;

            Tuple toInsert = Tuple.create();

            for (int i = 0; i < tuple.length; i++) {
                toInsert.set(columnNames[i], tuple[i]);
            }

            batch.add(toInsert);

            if (batch.size() == batchSize) {
                Collection<Tuple> duplicates = view.insertAll(null, batch);

                if (!duplicates.isEmpty()) {
                    throw new AssertionError("Duplicated rows detected: " + duplicates);
                }

                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            view.insertAll(null, batch);

            batch.clear();
        }
    }

    protected static List<List<?>> sql(String sql, Object... args) {
        return getAllFromCursor(
                ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine().query("PUBLIC", sql, args).get(0)
        );
    }
}
