/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.google.common.io.CharStreams;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;

/**
 * Provides utility methods to work with queries defined by the TPC-H benchmark.
 */
public final class TpchHelper {
    /** */
    public static final String INT32 = "INT32";

    /** */
    public static final String DECIMAL = "DECIMAL";

    /** */
    public static final String STRING = "VARCHAR";

    /** */
    public static final String DATE = "DATE";

    /** */
    private static final Pattern ID_PATTERN = Pattern.compile(", id = \\d+");

    /** */
    private static final Pattern HASH_PATTERN = Pattern.compile(", hash=-?\\d+]");

    /** */
    private TpchHelper() {
    }

    /**
     * Loads resource with given name as string.
     *
     * @param resource Name of the resource to load.
     * @return Resource as string.
     */
    public static String loadFromResource(String resource) {
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

    /** @return SQL test name. */
    public static String sqlTestName(Class<?> klass) {
        PlanChecker.PlansTest desc = plansTest(klass);

        if (desc.name().isEmpty())
            throw new IllegalStateException("Please, set test name with the @PlanTest(name=\"XXX\")");

        return desc.name();
    }

    /** @return Tables for the test. */
    public static Class<? extends Enum<? extends TpcTable>> tables(Class<?> klass) {
        PlanChecker.PlansTest desc = plansTest(klass);

        return desc.tables();
    }

    /** @return Test description annotation. */
    private static PlanChecker.PlansTest plansTest(Class<?> klass) {
        PlanChecker.PlansTest desc = klass.getAnnotation(PlanChecker.PlansTest.class);

        if (desc == null)
            throw new IllegalStateException("Test class must be annotated with @" + PlanChecker.PlansTest.class.getSimpleName());

        return desc;
    }

    /**
     * Execute SQL query.
     *
     * @param ignite Ignite.
     * @param sql SQL query.
     * @param params Query parameters.
     */
    public static List<List<?>> sql(Ignite ignite, String sql, Object... params) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setArgs(params);

        try (FieldsQueryCursor<List<?>> cur = ((IgniteEx)ignite).context().query().querySqlFields(qry, false)) {
            return cur.getAll();
        }
    }

    /**
     * Supported templates:
     * <ul>
     *     <li>{@code ", id = 123} replaced with the {@code ", id = {id}}</li>
     *     <li>{@code ", hash=123} replaced with the {@code ", hash={hash}}</li>
     *     <li>{@code "{INDEX_CASE:IDX1,IDX2}} expands to the plans where {INDEX_CASE....} replaced with each of the index from list.</li>
     *     <li>{@code "{ONEOF}} start and finish with the {@code {ONEOF}} tag. Cases separated with the line "=====".
     *     Creates as many plans as there are cases.
     *     </li>
     * </ul>
     *
     * @param plan Plan with templates
     * @return Expanded (templates replaced with the possible values). One of the plan must be equal to the one created by Ignite node.
     */
    public static List<String> expandTemplates(String plan) {
        return expandOneof(plan).stream()
            .flatMap(TpchHelper::expandIndexCase)
            .collect(Collectors.toList());
    }

    /** Expands plans. Replace {INDEX_CASE} tag with the possible indexes. */
    private static Stream<String> expandIndexCase(String plan) {
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

        return res.stream();
    }

    /** Expands plans. Replace {ONEOF} tag with the possible cases. */
    private static List<String> expandOneof(String plan) {
        String oneOf = "{ONEOF}";

        if (!plan.contains(oneOf))
            return Collections.singletonList(plan);

        List<StringBuffer> res = new ArrayList<>();

        Scanner sc = new Scanner(plan);

        StringBuffer beforeOneof = new StringBuffer();

        while (sc.hasNextLine()) {
            String line = sc.nextLine();

            if (line.trim().startsWith(oneOf))
                break;

            beforeOneof.append(line).append('\n');
        }

        boolean oneOfEnd = false;

        // Expanding first found oneof
        while(sc.hasNextLine() && !oneOfEnd) {
            StringBuffer oneofCase = new StringBuffer();

            while (sc.hasNextLine()) {
                String line = sc.nextLine();

                oneOfEnd = line.trim().equals(oneOf);

                if (line.trim().matches("^=+$") || oneOfEnd)
                    break;

                oneofCase.append(line).append('\n');
            }

            res.add(new StringBuffer(beforeOneof).append(oneofCase));
        }

        if (!oneOfEnd)
            throw new IllegalStateException(oneOf + " not closed");

        while (sc.hasNextLine()) {
            String line = sc.nextLine();

            for (StringBuffer expanded : res) {
                expanded.append(line).append('\n');
            }
        }

        // Recursively expanding next ONEOF.
        return res.stream().flatMap(p -> expandOneof(p.toString()).stream()).collect(Collectors.toList());
    }

    /** Replaces id and hash patterns. */
    public static String replaceIdAndHash(String plan) {
        plan = ID_PATTERN.matcher(plan).replaceAll(", id = {id}");
        return HASH_PATTERN.matcher(plan).replaceAll(", hash={hash}");
    }

    /** @return List of SQL queries from the script file. Comments are skipped. */
    public static List<String> scriptToQueries(String sqlScript) {
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
}
