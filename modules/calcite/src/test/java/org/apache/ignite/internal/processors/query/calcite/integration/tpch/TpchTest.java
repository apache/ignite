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

package org.apache.ignite.internal.processors.query.calcite.integration.tpch;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.junit.Test;

/**
 * Error in TPC-H Query #8 planning
 * <p>
 * Reproduced always.
 * <p>
 * If leave `calcite.volcano.dump.graphviz` = true and `calcite.volcano.dump.sets` = true, then this test
 * (if failed) would generate huge logs, allocate a lot of heap and hang the IDEA.
 */
//@WithSystemProperty(key = "calcite.volcano.dump.graphviz", value = "false")
//@WithSystemProperty(key = "calcite.volcano.dump.sets", value = "false")
public class TpchTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSqlConfiguration(new SqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()));

//        cfg.setTransactionConfiguration(new TransactionConfiguration().setTxAwareQueriesEnabled(true));

        return cfg;
    }

    /** */
    @Test
    public void test() throws Exception {
        TpchHelper.createTables(client);

        // OK: 0.7
        TpchHelper.fillTables(client, 0.85);

        TpchHelper.collectSqlStatistics(client);

        System.gc(); System.gc(); System.gc();

        System.err.println("TEST | sleeping"); Thread.sleep(3_000);

        try (FieldsQueryCursor<List<?>> cur = exec(TpchHelper.getQuery(17))) {
            for (List<?> next : cur)
                System.err.println("TEST | next: " + next.stream().map(Object::toString).collect(Collectors.joining(", ")));
        }
    }

    /** */
    protected FieldsQueryCursor<List<?>> exec(String sql) {
        if (log.isInfoEnabled())
            log.info("The test query:\n" + sql);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql.trim());

        qry.setDistributedJoins(true);

        return client.context().query().querySqlFields(qry, false);
    }
}