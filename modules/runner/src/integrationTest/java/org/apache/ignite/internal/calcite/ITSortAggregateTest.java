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

import java.util.List;

import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Sort aggregate integration test.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ITSortAggregateTest extends AbstractBasicIntegrationTest {
    /** */
    public static final int ROWS = 103;

    /** {@inheritDoc} */
    @Override protected void initTestData() {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "TEST")
            .columns(
                SchemaBuilders.column("ID", ColumnType.INT32).asNonNull().build(),
                SchemaBuilders.column("GRP0", ColumnType.INT32).asNullable().build(),
                SchemaBuilders.column("GRP1", ColumnType.INT32).asNullable().build(),
                SchemaBuilders.column("VAL0", ColumnType.INT32).asNullable().build(),
                SchemaBuilders.column("VAL1", ColumnType.INT32).asNullable().build()
            )
            .withPrimaryKey("ID")
            .withIndex(
                SchemaBuilders.sortedIndex("IDX")
                    .addIndexColumn("GRP0").done()
                    .addIndexColumn("GRP1").done()
                    .build()
            )
            .build();

        Table table = CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
            SchemaConfigurationConverter.convert(schTbl1, tblCh)
                .changeReplicas(2)
                .changePartitions(10)
        );

        RecordView<Tuple> view = table.recordView();
        for (int i = 0; i < ROWS; i++) {
            view.insert(
                Tuple.create()
                    .set("ID", i)
                    .set("GRP0", i / 10)
                    .set("GRP1", i / 100)
                    .set("VAL0", 1)
                    .set("VAL1", 2)
            );
        }
    }

    /** */
    @Test
    public void mapReduceAggregate() {
        List<List<?>> res = sql(
            "SELECT /*+ DISABLE_RULE('HashAggregateConverterRule') */" +
            "SUM(val0), SUM(val1), grp0 FROM TEST " +
            "GROUP BY grp0 " +
            "HAVING SUM(val1) > 10"
        );

        assertEquals(ROWS / 10, res.size());

        res.forEach(r -> {
            Integer s0 = (Integer)r.get(0);
            Integer s1 = (Integer)r.get(1);

            assertEquals(s0 * 2, (int)s1);
        });
    }
}
