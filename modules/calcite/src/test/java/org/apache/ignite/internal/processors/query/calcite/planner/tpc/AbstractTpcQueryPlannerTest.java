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
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import com.google.common.io.CharStreams;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.calcite.integration.tpch.TpchHelper;
import org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.integration.tpch.TpchHelper.sql;
import static org.apache.logging.log4j.util.Cast.cast;

/**
 * Abstract test class to ensure a planner generates optimal plan for TPC queries.
 */
public abstract class AbstractTpcQueryPlannerTest extends AbstractPlannerTest {
    private static final boolean updatePlan = false;

    private static final Pattern COSTS_PATTERN = Pattern.compile("\\s+est: \\(rows=\\d+\\)");

    private static IgniteEx srv;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srv = startGrid();

        TpcTable[] tables = cast(tables().getEnumConstants());

        for (TpcTable table : tables)
            scriptToQueries(table.ddlScript()).forEach(q -> sql(srv, q));
    }

    @Test
    public void testQueries() throws Exception {
        Files.list(Path.of("./src/test/resources", name()))
            .filter(p -> p.toString().endsWith(".sql"))
            .map(this::queryId)
            .forEach(this::validateQueryPlan);
    }

    private void validateQueryPlan(String queryId) {
        List<String> actualPlans = queryPlan(queryString(queryId));

        if (updatePlan) {
            updateQueryPlan(queryId, actualPlans);
            return;
        }

        String[] expectedPlans = expectedQueryPlan(queryId).split("----(\\r\\n|\\n|\\r)");

        assert expectedPlans.length == actualPlans.size() : "Unexpected number of plans, got: " + actualPlans.size()
            + ", expected: " + expectedPlans.length;

        int pos = 0;

        for (String actualPlan : actualPlans) {
            String expectedPlan = expectedPlans[pos++];

            // Internally, costs are represented by double values and conversion to exact numeric representation
            // may differs from JVM to JVM.
            // https://www.oracle.com/java/technologies/javase/19-relnote-issues.html
            // Cut-off costs, which may differ between runs, before comparing plans.
            expectedPlan = COSTS_PATTERN.matcher(expectedPlan).replaceAll("");
            actualPlan = COSTS_PATTERN.matcher(actualPlan).replaceAll("");

            assertEquals(expectedPlan, actualPlan);
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

    static void updateQueryPlan(String queryId, Path targetDirectory, List<String> newPlans) {
        // A targetDirectory must be specified by hand when expected plans are generated.
        if (targetDirectory == null) {
            throw new RuntimeException("Please provide target directory to where save generated plans."
                + " Usually plans are kept in resource folder of tests within the same module.");
        }

        // variant query ends with "v"
        boolean variant = queryId.endsWith("v");
        int numericId;

        if (variant) {
            String idString = queryId.substring(0, queryId.length() - 1);
            numericId = Integer.parseInt(idString);
        } else {
            numericId = Integer.parseInt(queryId);
        }

        Path planLocation;
        if (variant) {
            planLocation = targetDirectory.resolve(String.format("variant_q%d.plan", numericId));
        } else {
            planLocation = targetDirectory.resolve(String.format("q%s.plan", numericId));
        }

        try {
            Files.createDirectories(targetDirectory);

            String plans = String.join("----" + System.lineSeparator(), newPlans);
            Files.writeString(planLocation, plans);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    String expectedQueryPlan(String queryId) {
        // variant query ends with "v"
        boolean variant = queryId.endsWith("v");
        int numericId;

        if (variant) {
            String idString = queryId.substring(0, queryId.length() - 1);
            numericId = Integer.parseInt(idString);
        } else {
            numericId = Integer.parseInt(queryId);
        }

        if (variant) {
            var variantQueryFile = String.format("%s/variant_q%d.plan", name(), numericId);
            return loadFromResource(variantQueryFile);
        } else {
            var queryFile = String.format("%s/q%s.plan", name(), numericId);
            return loadFromResource(queryFile);
        }
    }

    private String queryId(Path p) {
        String name = p.getFileName().toString();

        return name.substring(1).replace(".sql", "");
    }

    private List<String> queryPlan(String sqlScript) {
        return scriptToQueries(sqlScript).stream().map(qry -> {
            List<List<?>> explain = sql(srv, "EXPLAIN PLAN FOR " + qry);

            StringBuilder plan = new StringBuilder();

            for (int i = 0; i < explain.size(); i++) {
                assertEquals(1, explain.get(i).size());

                plan.append(explain.get(i).get(0));
            }

            return plan.toString();
        }).collect(Collectors.toList());
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

    abstract String queryString(String queryId);

    abstract void updateQueryPlan(String queryId, List<String> newPlans);

    /** Returns name of the SQL test. */
    abstract String name();

    abstract Class<? extends Enum<? extends TpcTable>> tables();
}
