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

import java.util.Collection;
import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
@WithSystemProperty(key = "IGNITE_CALCITE_PLANNER_TIMEOUT", value = "300000")
public class TpchTest extends AbstractBasicIntegrationTest {
    /** */
    private static final int CNT = 1;

    /** */
    private static final int WARM = 3;

    /** Query ID. */
    @Parameterized.Parameter
    public int qryId;

    /** */
    @Parameterized.Parameters(name = "queryId={0}")
    public static Collection<Object> params() {
        return F.asList(5);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        TpchHelper.createTables(client);

        TpchHelper.fillTables(client, 0.1);

        TpchHelper.collectSqlStatistics(client);
    }

    /** {@inheritDoc} */
    @Override protected boolean destroyCachesAfterTest() {
        return false;
    }

    /**
     * Test the TPC-H query can be planned and executed.
     */
    @Test
    public void test() {
        if (qryId == 15) {
            sql("create OR REPLACE view revenue0 as\n" +
                "    select\n" +
                "        l_suppkey as supplier_no,\n" +
                "        sum(l_extendedprice * (1 - l_discount)) as total_revenue\n" +
                "    from\n" +
                "        lineitem\n" +
                "    where\n" +
                "        l_shipdate >= date '1996-01-01'\n" +
                "        and l_shipdate < TIMESTAMPADD(MONTH, 3, date '1996-01-01')\n" +
                "    group by\n" +
                "        l_suppkey;");
        }

        long avg = 0;
        long avgCached = 0;
        long plannning = 0;

        for (int i = 0; i < CNT; ++i) {
            if (i == 0)
                plannning = System.nanoTime();

            long tt = sqlTiming(TpchHelper.getQuery(qryId));

            if (i == 0)
                plannning = U.nanosToMillis(System.nanoTime() - plannning);

            avg += tt;

            if (i >= WARM - 1)
                avgCached += tt;
        }

        avg /= CNT;
        avgCached /= (CNT - WARM);

        if (log.isInfoEnabled())
            log.info("TEST | avg: " + avg + ", planning: " + plannning + ", avgCached: " + avgCached);
    }

    /** */
    protected long sqlTiming(String sql, Object... params) {
        return sqlTiming(client, sql, params);
    }

    /** */
    protected long sqlTiming(IgniteEx ignite, String sql, Object... params) {
        CalciteQueryProcessor qProc = queryProcessor(ignite);
        QueryContext qCtx = queryContext();

        long t = System.nanoTime();

        List<FieldsQueryCursor<List<?>>> cur = qProc.query(qCtx, "PUBLIC", sql, params);

        try (QueryCursor<List<?>> srvCursor = cur.get(0)) {
            srvCursor.getAll();
        }
        finally {
            t = U.nanosToMillis(System.nanoTime() - t);
        }

        if (log.isInfoEnabled())
            log.info("TEST | Query millis: " + t);

        return t;
    }
}
