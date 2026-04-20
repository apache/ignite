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
import com.google.common.io.CharStreams;

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
     * Loads a TPC-H query given a query identifier. Query identifier can be in the following forms:
     * <ul>
     *     <li>query id - a number, e.g. {@code 14}. Queries are stored in files named q{id}.sql.</li>
     *     <li>query variant id, a number and 'v' suffix - {@code 12v}. Query variants are stored in files
     *     named variant_q{numeric_id}.sql.</li>
     * </ul>
     *
     * @param queryId The identifier of a query.
     * @return  An SQL query.
     */
    public static String getQuery(String queryId) {
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
            var variantQueryFile = String.format("tpch/variant_q%d.sql", numericId);
            return loadFromResource(variantQueryFile);
        } else {
            var queryFile = String.format("tpch/q%s.sql", numericId);
            return loadFromResource(queryFile);
        }
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
}
