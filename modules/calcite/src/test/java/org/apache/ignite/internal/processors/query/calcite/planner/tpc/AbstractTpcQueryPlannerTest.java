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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
import org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.tpc.PlanChecker.AfterPlansTest;
import org.apache.ignite.internal.processors.query.calcite.planner.tpc.PlanChecker.BeforePlansTest;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.query.calcite.planner.tpc.TpchHelper.sql;
import static org.apache.logging.log4j.util.Cast.cast;

/**
 * Abstract test class to ensure a planner generates optimal plan for TPC queries.
 */
public class AbstractTpcQueryPlannerTest extends AbstractPlannerTest {
    private static final boolean UPDATE_PLAN = false;

    private static final Pattern ID_PATTERN = Pattern.compile(", id = \\d+");
    private static final Pattern HASH_PATTERN = Pattern.compile(", hash=-?\\d+]");

    private static IgniteEx srv;

    @Parameterized.Parameter
    public String queryId;

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

    @AfterPlansTest
    public static void stopAll(Class<?> testClass) {
        if (srv != null) {
            srv.close();

            srv = null;
        }
    }

    @Test
    public void testQuery() {
        List<String> actualPlans = queryPlan(loadFromResource(String.format(TpchHelper.name(getClass()) + "/%s.sql", queryId)));

        if (UPDATE_PLAN) {
            updateQueryPlan(queryId, actualPlans);
            return;
        }

        String[] expectedPlans = loadFromResource(String.format("%s/%s.plan", TpchHelper.name(getClass()), queryId))
            .split("----(\\r\\n|\\n|\\r)");

        assert expectedPlans.length == actualPlans.size() : "Unexpected number of plans, got: " + actualPlans.size()
            + ", expected: " + expectedPlans.length;

        int pos = 0;

        for (String actualPlan : actualPlans) {
            boolean match = false;

            String expectedPlan = replaceIdAndHash(expectedPlans[pos++]);

            for (String possiblePlan : expandWithPossibleIndexes(expectedPlan)) {
                if (possiblePlan.equals(actualPlan)) {
                    match = true;

                    break;
                }
            }

            if (!match) {
                // This assertion will print nice diff in IDE that will help to investigate.
                // Test will fail anyway.
                assertEquals(expectedPlan, actualPlan);

                assert false : "Should not happen";
            }
        }
    }

    static String loadFromResource(String resource) {
        try (InputStream is = TpchHelper.class.getClassLoader().getResourceAsStream(resource)) {
            if (is == null) {
                throw new IllegalArgumentException("Resource does not exist: " + resource);
            }
            try (InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
                return CharStreams.toString(reader);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("I/O operation failed: " + resource, e);
        }
    }

    private static <T> T invoke(Method method, Object... arguments) {
        try {
            return (T) method.invoke(null, arguments);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateQueryPlan(String queryId, List<String> newPlans) {
        Path targetDirectory = Path.of("./src/test/resources/" + TpchHelper.name(getClass()));

        // A targetDirectory must be specified by hand when expected plans are generated.
        if (targetDirectory == null) {
            throw new RuntimeException("Please provide target directory to where save generated plans."
                + " Usually plans are kept in resource folder of tests within the same module.");
        }

        try {
            Files.createDirectories(targetDirectory);

            String plans = String.join("----" + System.lineSeparator(), newPlans);
            Files.writeString(targetDirectory.resolve(String.format("%s.plan", queryId)), plans);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private List<String> queryPlan(String sqlScript) {
        CalciteQueryProcessor engine = (CalciteQueryProcessor)srv.context().query().defaultQueryEngine();

        Map<String, IgniteSchema> schemas = GridTestUtils.getFieldValue(engine.schemaHolder(), "igniteSchemas");

        return scriptToQueries(sqlScript).stream().map(qry -> {
            try {
                return RelOptUtil.toString(physicalPlan(plannerCtx(sqlScript, schemas.values(), null)), SqlExplainLevel.ALL_ATTRIBUTES);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).map(AbstractTpcQueryPlannerTest::replaceIdAndHash).collect(Collectors.toList());
    }

    private static List<String> scriptToQueries(String sqlScript) {
        List<String> queries = new ArrayList<>();

        Scanner sc = new Scanner(sqlScript);

        StringBuilder current = new StringBuilder();

        while (sc.hasNextLine()) {
            String line = sc.nextLine().trim();

            if (line.startsWith("--") || line.isEmpty())
                continue;

            current.append(line).append('\n');

            if (line.endsWith(";")) {
                queries.add(current.toString());

                current = new StringBuilder();
            }
        }

        if (current.length() > 0)
            queries.add(current.toString());

        return queries;
    }

    private static List<String> expandWithPossibleIndexes(String plan) {
        List<String> res = new ArrayList<>();

        res.add(plan);

        while (true) {
            String idxCaseStart = "{INDEX_CASE:";

            if (res.get(0).contains(idxCaseStart)) {
                res = res.stream().flatMap(p -> {
                    int start = p.indexOf(idxCaseStart);
                    int end = p.indexOf('}', start);

                    assert start != -1 && end != -1;

                    String[] idxs = p.substring(start + idxCaseStart.length(), end).split(",");

                    return Arrays.stream(idxs).map(idx -> p.substring(0, start) + idx + p.substring(end + 1));
                }).collect(Collectors.toList());
            }
            else
                break;
        }

        return res;
    }

    private static String replaceIdAndHash(String plan) {
        plan = ID_PATTERN.matcher(plan).replaceAll(", id = {id}");
        return HASH_PATTERN.matcher(plan).replaceAll(", hash={hash}");
    }

    /** {@inheritDoc} */
    @Override protected boolean isSafeTopology() {
        return false;
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
