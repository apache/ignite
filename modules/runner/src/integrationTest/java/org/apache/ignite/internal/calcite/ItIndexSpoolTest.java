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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

import java.util.List;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.processors.query.calcite.QueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.SqlCursor;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Index spool test.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItIndexSpoolTest extends AbstractBasicIntegrationTest {
    /**
     *
     */
    private static final IgniteLogger LOG = IgniteLogger.forClass(AbstractBasicIntegrationTest.class);

    /**
     *
     */
    @AfterEach
    protected void cleanUp() {
        if (LOG.isInfoEnabled()) {
            LOG.info("Start cleanUp()");
        }

        CLUSTER_NODES.get(0).tables().tables().stream()
                .map(Table::tableName)
                .forEach(CLUSTER_NODES.get(0).tables()::dropTable);

        if (LOG.isInfoEnabled()) {
            LOG.info("End cleanUp()");
        }
    }

    /**
     *
     */
    @ParameterizedTest(name = "tableSize=" + ARGUMENTS_PLACEHOLDER)
    @ValueSource(ints = {1, 10, 512, 513, 2000})
    public void test(int rows) {
        prepareDataSet(rows);

        QueryProcessor engine = ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine();

        List<SqlCursor<List<?>>> cursors = engine.query(
                "PUBLIC",
                "SELECT /*+ DISABLE_RULE('NestedLoopJoinConverter', 'MergeJoinConverter') */"
                        + "T0.val, T1.val FROM TEST0 as T0 "
                        + "JOIN TEST1 as T1 on T0.jid = T1.jid "
        );

        List<List<?>> res = getAllFromCursor(cursors.get(0));

        assertThat(res.size(), is(rows));

        res.forEach(r -> assertThat(r.get(0), is(r.get(1))));
    }

    private Table createTable(String tableName) {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", tableName)
                .columns(
                        SchemaBuilders.column("ID", ColumnType.INT32).asNonNull().build(),
                        SchemaBuilders.column("JID", ColumnType.INT32).asNullable().build(),
                        SchemaBuilders.column("VAL", ColumnType.string()).asNullable().build()
                )
                .withPrimaryKey("ID")
                .withIndex(
                        SchemaBuilders.sortedIndex(tableName + "_JID_IDX").addIndexColumn("JID").done().build()
                )
                .build();

        return CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl1, tblCh)
                        .changeReplicas(2)
                        .changePartitions(10)
        );
    }

    private void prepareDataSet(int rowsCount) {
        Object[][] dataRows = new Object[rowsCount][];

        for (int i = 0; i < rowsCount; i++) {
            dataRows[i] = new Object[]{i, i + 1, "val_" + i};
        }

        for (String name : List.of("TEST0", "TEST1")) {
            Table tbl = createTable(name);

            insertData(tbl, new String[]{"ID", "JID", "VAL"}, dataRows);
        }
    }
}
