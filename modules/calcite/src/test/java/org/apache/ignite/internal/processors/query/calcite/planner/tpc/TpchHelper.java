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
import java.nio.file.Path;
import java.util.List;
import com.google.common.io.CharStreams;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;

/**
 * Provides utility methods to work with queries defined by the TPC-H benchmark.
 */
public final class TpchHelper {
    public static final String INT32 = "INT32";

    public static final String DECIMAL = "DECIMAL";

    public static final String STRING = "VARCHAR";

    public static final String DATE = "DATE";


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

    public static String queryId(Path p) {
        return p.getFileName().toString().replace(".sql", "");
    }

    public static String name(Class<?> klass) {
        PlanChecker.PlansTest desc = plansTest(klass);

        if (desc.name().isEmpty())
            throw new IllegalStateException("Please, set test name with the @PlanTest(name=\"XXX\")");

        return desc.name();
    }

    public static Class<? extends Enum<? extends TpcTable>> tables(Class<?> klass) {
        PlanChecker.PlansTest desc = plansTest(klass);

        return desc.tables();
    }

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
}
