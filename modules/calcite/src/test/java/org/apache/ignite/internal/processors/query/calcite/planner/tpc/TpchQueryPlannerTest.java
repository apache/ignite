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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import com.google.common.io.CharStreams;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.tpch.TpchHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests ensures a planner generates optimal plan for TPC-H queries.
 *
 * @code org.apache.ignite.internal.sql.engine.benchmarks.TpchParseBenchmark
 */
@RunWith(Parameterized.class)
public class TpchQueryPlannerTest extends AbstractBasicIntegrationTest {
    /** Set to {@code true} to write plan files, instead of checking. */
    private static final boolean UPDATE_PLAN = false;

    /** */
    public static final String TPCH = "tpch";

    /** */
    public static final String RSRC_DIR = "./src/test/resources/" + TPCH;

    @Parameterized.Parameters(name = "queryId={0}")
    public static Collection<String> params() throws IOException {
        return Files.list(Path.of(RSRC_DIR))
            .map(p -> p.getFileName().toString())
            .filter(p -> p.endsWith(".sql") && !p.endsWith("ddl.sql"))
            .map(p -> p.replace(".sql", ""))
            .sorted(Comparator.comparingInt(p -> Integer.parseInt(p.replace("variant_q", "").replace("q", ""))))
            .collect(Collectors.toList());
    }

    /** Query id. */
    @Parameterized.Parameter
    public String qryId;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteConfiguration cfg = getConfiguration("server");

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration().setDefault(true));

        TpchHelper.createTables(grid(0));

        TpchHelper.fillTables(grid(0), 0.01);

        TpchHelper.collectSqlStatistics(grid(0));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** Test single query. */
    @Test
    public void testQuery() {
        String actualPlan = queryPlan();

        if (UPDATE_PLAN) {
            updatePlan(actualPlan);

            return;
        }

        PlanTemplate pt = new PlanTemplate(loadFromResource(qryId + ".plan"));

        if (!pt.match(actualPlan)) {
            // This assertion will print nice diff in IDE that will help to investigate.
            // Test will fail anyway.
            assertEquals(pt.template, actualPlan);

            assert false : "Should not happen";
        }
    }

    /** */
    private String queryPlan() {
        List<List<?>> res = sql(grid(0), "EXPLAIN PLAN FOR " + loadFromResource(qryId + ".sql"));

        assertEquals(1, res.size());

        return new PlanTemplate(res.get(0).get(0).toString()).template;
    }

    /** */
    private void updatePlan(String newPlan) {
        Path targetDir = Path.of(RSRC_DIR);

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

    /**
     * Loads resource with given name as string.
     *
     * @param rsrc Name of the resource to load.
     * @return Resource as string.
     */
    private static String loadFromResource(String rsrc) {
        try (InputStream is = TpchQueryPlannerTest.class.getClassLoader().getResourceAsStream(TPCH + "/" + rsrc)) {
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

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected boolean destroyCachesAfterTest() {
        return false;
    }
}
