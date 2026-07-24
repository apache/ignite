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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import com.google.common.io.CharStreams;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.integration.tpch.TpchHelper;
import org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.tpc.PlanChecker.AfterPlansTest;
import org.apache.ignite.internal.processors.query.calcite.planner.tpc.PlanChecker.BeforePlansTest;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.query.calcite.planner.tpc.PlanChecker.RSRC_DIR;
import static org.apache.ignite.internal.processors.query.calcite.planner.tpc.PlanChecker.sqlTestName;

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
    public String qryId;

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

        TpchHelper.createTables(srv);

/*
        TpchHelper.fillTables(srv, 0.01);

        TpchHelper.collectSqlStatistics(srv);
*/
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
    public void testQuery() throws Exception {
        String actualPlan = queryPlan();

        if (UPDATE_PLAN) {
            updatePlan(actualPlan);

            return;
        }

        PlanTemplate pt = new PlanTemplate(loadFromResource(String.format("%s/%s.plan", sqlTestName(getClass()), qryId)));

        if (!pt.match(actualPlan)) {
            // This assertion will print nice diff in IDE that will help to investigate.
            // Test will fail anyway.
            assertEquals(pt.template, actualPlan);

            assert false : "Should not happen";
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean isSafeTopology() {
        return false;
    }

    /** */
    private String queryPlan() throws Exception {
        Map<String, IgniteSchema> schemas = GridTestUtils.getFieldValue(
            ((CalciteQueryProcessor)srv.context().query().defaultQueryEngine()).schemaHolder(),
            "igniteSchemas"
        );

        PlanningContext ctx = plannerCtx(loadFromResource(sqlTestName(getClass()) + "/" + qryId + ".sql"), schemas.values(), null);

        return new PlanTemplate(RelOptUtil.toString(physicalPlan(ctx), SqlExplainLevel.ALL_ATTRIBUTES)).template;
    }

    /** */
    private void updatePlan(String newPlan) {
        Path targetDir = Path.of(RSRC_DIR + sqlTestName(getClass()));

        // A targetDirectory must be specified by hand when expected plans are generated.
        if (targetDir == null) {
            throw new RuntimeException("Please provide target directory to where save generated plans."
                + " Usually plans are kept in resource folder of tests within the same module.");
        }

        try {
            Files.createDirectories(targetDir);

            Files.writeString(targetDir.resolve(String.format("%s.plan", qryId)), newPlan);
        }
        catch (Exception e) {
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

    /**
     * Loads resource with given name as string.
     *
     * @param rsrc Name of the resource to load.
     * @return Resource as string.
     */
    private static String loadFromResource(String rsrc) {
        if (rsrc.startsWith(RSRC_DIR))
            rsrc = rsrc.substring(RSRC_DIR.length() + 1);

        try (InputStream is = AbstractTpcQueryPlannerTest.class.getClassLoader().getResourceAsStream(rsrc)) {
            if (is == null)
                throw new IllegalArgumentException("Resource does not exist: " + rsrc);

            try (InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
                return CharStreams.toString(reader);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("I/O operation failed: " + rsrc, e);
        }
    }
}
