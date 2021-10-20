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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.ITUtils;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.calcite.util.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.QueryProcessor;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.ignite.internal.calcite.util.Commons.getAllFromCursor;

/**
 *
 */
@ExtendWith(WorkDirectoryExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AbstractBasicIntegrationTest {
    /** */
    private static final IgniteLogger LOG = IgniteLogger.forClass(AbstractBasicIntegrationTest.class);

    /** Nodes bootstrap configuration. */
    private static final Map<String, String> NODES_BOOTSTRAP_CFG = new LinkedHashMap<>() {{
        put("node0", "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ \"node0\" ]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3344,\n" +
            "    \"nodeFinder\": {\n" +
            "      \"netClusterNodes\": [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}");

        put("node1", "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ \"node0\" ]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3345,\n" +
            "    \"nodeFinder\":{\n" +
            "      \"netClusterNodes\": [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}");

        put("node2", "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ \"node0\" ]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3346,\n" +
            "    \"nodeFinder\":{\n" +
            "      \"netClusterNodes\": [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}");
    }};

    /** Cluster nodes. */
    protected static final List<Ignite> CLUSTER_NODES = new ArrayList<>();

    /** Work directory */
    @WorkDirectory
    private Path workDir;

    @BeforeEach
    protected void bootstrap() {
        if (!CLUSTER_NODES.isEmpty())
            return;

        NODES_BOOTSTRAP_CFG.forEach((nodeName, configStr) ->
            CLUSTER_NODES.add(IgnitionManager.start(nodeName, configStr, workDir.resolve(nodeName)))
        );

        initTestData();
    }

    /** */
    @AfterAll
    static void tearDown() throws Exception {
        if (CLUSTER_NODES.isEmpty())
            return;

        if (LOG.isInfoEnabled())
            LOG.info("Start tearDown()");

        IgniteUtils.closeAll(ITUtils.reverse(CLUSTER_NODES));

        CLUSTER_NODES.clear();

        if (LOG.isInfoEnabled())
            LOG.info("End tearDown()");
    }

    /** */
    protected QueryChecker assertQuery(String qry) {
        return new QueryChecker(qry) {
            @Override protected QueryProcessor getEngine() {
                return ((IgniteImpl)CLUSTER_NODES.get(0)).queryEngine();
            }
        };
    }

    /** */
    protected Table createAndPopulateTable() {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "PERSON").columns(
            SchemaBuilders.column("ID", ColumnType.INT32).asNonNull().build(),
            SchemaBuilders.column("NAME", ColumnType.string()).asNullable().build(),
            SchemaBuilders.column("SALARY", ColumnType.DOUBLE).asNullable().build()
        ).withPrimaryKey("ID").build();

        Table tbl = CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
            SchemaConfigurationConverter.convert(schTbl1, tblCh)
                .changeReplicas(1)
                .changePartitions(10)
        );

        int idx = 0;

        insertData(tbl, new String[] {"ID", "NAME", "SALARY"}, new Object[][] {
            {idx++, "Igor", 10d},
            {idx++, null, 15d},
            {idx++, "Ilya", 15d},
            {idx++, "Roma", 10d},
            {idx, "Roma", 10d}
        });

        return tbl;
    }

    protected void insertData(Table table, String[] columnNames, Object[]... tuples) {
        RecordView<Tuple> view = table.recordView();

        int batchSize = 128;

        List<Tuple> batch = new ArrayList<>(batchSize);
        for (Object[] tuple : tuples) {
            assert tuple != null && tuple.length == columnNames.length;

            Tuple toInsert = Tuple.create();

            for (int i = 0; i < tuple.length; i++)
                toInsert.set(columnNames[i], tuple[i]);

            batch.add(toInsert);

            if (batch.size() == batchSize) {
                Collection<Tuple> duplicates = view.insertAll(batch);

                if (!duplicates.isEmpty())
                    throw new AssertionError("Duplicated rows detected: " + duplicates);

                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            view.insertAll(batch);

            batch.clear();
        }
    }

    protected List<List<?>> sql(String sql, Object... args) {
        return getAllFromCursor(
            ((IgniteImpl)CLUSTER_NODES.get(0)).queryEngine().query("PUBLIC", sql, args).get(0)
        );
    }

    protected void initTestData() {

    }
}
