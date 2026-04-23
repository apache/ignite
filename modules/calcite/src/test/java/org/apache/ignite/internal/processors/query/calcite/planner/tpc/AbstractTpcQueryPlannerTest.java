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

package org.apache.ignite.internal.processors.query.calcite.planner.tpc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.tpc.PlanChecker.AfterPlansTest;
import org.apache.ignite.internal.processors.query.calcite.planner.tpc.PlanChecker.BeforePlansTest;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.query.calcite.planner.tpc.TpchHelper.loadFromResource;
import static org.apache.ignite.internal.processors.query.calcite.planner.tpc.TpchHelper.scriptToQueries;
import static org.apache.ignite.internal.processors.query.calcite.planner.tpc.TpchHelper.sql;
import static org.apache.ignite.internal.processors.query.calcite.planner.tpc.TpchHelper.sqlTestName;
import static org.apache.logging.log4j.util.Cast.cast;

/**
 * Abstract test class to ensure a planner generates expected plan for TPC queries.
 */
public class AbstractTpcQueryPlannerTest extends AbstractPlannerTest {
    /** Set to {@code true} to write plan files, instead of checking. */
    private static final boolean UPDATE_PLAN = false;

    /** Server node to run queries on. */
    private static IgniteEx srv;

    /** Query id. Set by {@link PlanChecker}. */
    @Parameterized.Parameter
    public String queryId;

    /** Run once, before all queries check. */
    @BeforePlansTest
    public static void startAll(Class<?> testClass) throws Exception {
        AbstractTpcQueryPlannerTest mock = new AbstractTpcQueryPlannerTest();

        mock.beforeFirstTest();

        IgniteConfiguration cfg = mock.getConfiguration("server");

        cfg.getSqlConfiguration()
            .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration().setDefault(true));

        srv = (IgniteEx)IgnitionEx.start(cfg);

        srv.getOrCreateCache(new CacheConfiguration<>("mock")
            .setSqlFunctionClasses(AbstractTpcQueryPlannerTest.TpchUDF.class)
            .setSqlSchema("PUBLIC"));

        TpcTable[] tables = cast(TpchHelper.tables(testClass).getEnumConstants());

        for (TpcTable table : tables)
            scriptToQueries(table.ddlScript()).forEach(q -> sql(srv, q));
    }

    /** Run once, after all queries check. */
    @AfterPlansTest
    public static void stopAll(Class<?> testClass) {
        if (srv != null) {
            srv.close();

            srv = null;
        }
    }

    /** Test single query. */
    @Test
    public void testQuery() {
        String actualPlan = queryPlan();

        if (UPDATE_PLAN) {
            updatePlan(actualPlan);

            return;
        }

        boolean match = false;

        String expPlan = TpchHelper.replaceIdAndHash(loadFromResource(String.format("%s/%s.plan", sqlTestName(getClass()), queryId)));

        for (String possiblePlan : TpchHelper.expandTemplates(expPlan)) {
            if (possiblePlan.equals(actualPlan)) {
                match = true;

                break;
            }
        }

        if (!match) {
            // This assertion will print nice diff in IDE that will help to investigate.
            // Test will fail anyway.
            assertEquals(expPlan, actualPlan);

            assert false : "Should not happen";
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean isSafeTopology() {
        return false;
    }

    /** */
    private String queryPlan() {
        CalciteQueryProcessor engine = (CalciteQueryProcessor)srv.context().query().defaultQueryEngine();

        Map<String, IgniteSchema> schemas = GridTestUtils.getFieldValue(engine.schemaHolder(), "igniteSchemas");

        List<String> res = scriptToQueries(loadFromResource(sqlTestName(getClass()) + "/" + queryId + ".sql")).stream().map(qry -> {
            try {
                return RelOptUtil.toString(physicalPlan(plannerCtx(qry, schemas.values(), null)), SqlExplainLevel.ALL_ATTRIBUTES);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        })
            .map(TpchHelper::replaceIdAndHash)
            .collect(Collectors.toList());

        assertEquals(1, res.size());

        return res.get(0);
    }

    /** */
    private void updatePlan(String newPlan) {
        Path targetDirectory = Path.of("./src/test/resources/" + sqlTestName(getClass()));

        // A targetDirectory must be specified by hand when expected plans are generated.
        if (targetDirectory == null) {
            throw new RuntimeException("Please provide target directory to where save generated plans."
                + " Usually plans are kept in resource folder of tests within the same module.");
        }

        try {
            Files.createDirectories(targetDirectory);

            Files.writeString(targetDirectory.resolve(String.format("%s.plan", queryId)), newPlan);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    public static class TpchUDF {
        /** */
        @QuerySqlFunction(alias = "SUBSTR")
        public static String substr(String str, int from, int cnt) {
            return str.substring(from, from + cnt);
        }
    }
}
