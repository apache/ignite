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
 *
 */

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.query.DistributedSqlConfiguration;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_USER_QUERIES_REG_NAME;

/**
 * Test queries timeout.
 */
public class TimeoutIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static final int SLEEP_PER_ROW = 1;

    /** */
    private static final int ROW_CNT = 3_000;

    /** */
    private static final int TIMEOUT = SLEEP_PER_ROW * ROW_CNT / 2;

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration().setDefault(true),
            new IndexingQueryEngineConfiguration()); // Add both engines to check common properties sharing.

        return cfg;
    }

    /** */
    @Test
    public void testLocalTimeoutSetByDistributedProperty() {
        checkTimeoutSetByDistributedProperty(grid(0));
    }

    /** */
    @Test
    public void testRemoteTimeoutSetByDistributedProperty() {
        checkTimeoutSetByDistributedProperty(client);
    }

    /** */
    @Test
    public void testLocalTimeoutSetByFieldsQuery() {
        checkTimeoutSetByFieldsQuery(grid(0));
    }

    /** */
    @Test
    public void testRemoteTimeoutSetByFieldsQuery() {
        checkTimeoutSetByFieldsQuery(client);
    }

    /** */
    private void checkTimeoutSetByDistributedProperty(IgniteEx node) {
        checkTimeout(node, new IgniteClosureX<String, List<List<?>>>() {
            @Override public List<List<?>> applyx(String sql) throws IgniteCheckedException {
                DistributedSqlConfiguration distrCfg = queryProcessor(node).distributedConfiguration();

                try {
                    distrCfg.defaultQueryTimeout(TIMEOUT).get();

                    return sql(node, sql);
                }
                finally {
                    distrCfg.defaultQueryTimeout(DistributedSqlConfiguration.DFLT_QRY_TIMEOUT).get();
                }
            }
        });
    }

    /** */
    private void checkTimeoutSetByFieldsQuery(IgniteEx node) {
        checkTimeout(node, new IgniteClosureX<String, List<List<?>>>() {
            @Override public List<List<?>> applyx(String sql) {
                IgniteCache<?, ?> cache = node.getOrCreateCache(DEFAULT_CACHE_NAME);

                return cache.query(new SqlFieldsQuery(sql).setTimeout(TIMEOUT, TimeUnit.MILLISECONDS)).getAll();
            }
        });
    }

    /**
     *
     */
    private void checkTimeout(IgniteEx node, IgniteClosureX<String, List<List<?>>> qryExecutor) {
        MetricRegistry mreg = node.context().metric().registry(SQL_USER_QUERIES_REG_NAME);
        mreg.reset();

        sql("CREATE TABLE person (id int, val varchar)");

        try {
            CalciteQueryProcessor srvEngine = queryProcessor(grid(0));

            IgniteCacheTable oldTbl = (IgniteCacheTable)srvEngine.schemaHolder().schema("PUBLIC").getTable("PERSON");

            IgniteCacheTable newTbl = new CacheTableImpl(grid(0).context(), oldTbl.descriptor()) {
                @Override public <Row> Iterable<Row> scan(
                    ExecutionContext<Row> execCtx,
                    ColocationGroup grp,
                    @Nullable ImmutableBitSet usedColumns
                ) {
                    return F.iterator(super.scan(execCtx, grp, usedColumns), r -> {
                        doSleep(SLEEP_PER_ROW);
                        return r;
                    }, true);
                }
            };

            // Replace original table on server node to "slow" table.
            srvEngine.schemaHolder().schema("PUBLIC").add("PERSON", newTbl);

            String qry = "SELECT * FROM person";

            // Build plan and mapping to reduce execution time by qryExecutor.
            sql(node, qry);

            // Check query works when timeout is set, but not reached.
            qryExecutor.apply(qry);

            for (int i = 0; i < ROW_CNT; i++)
                sql("INSERT INTO person (id, val) VALUES (?, ?)", i, "val" + i);

            GridTestUtils.assertThrowsAnyCause(log, () -> qryExecutor.apply(qry),
                QueryCancelledException.class, "The query was timed out.");
        }
        finally {
            sql("DROP TABLE person");
        }

        assertEquals(1, ((LongMetric)mreg.findMetric("canceled")).value());
    }
}
