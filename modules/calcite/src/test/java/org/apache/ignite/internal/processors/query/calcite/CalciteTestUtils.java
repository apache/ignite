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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

public class CalciteTestUtils {
    /** */
    public static List<List<?>> executeSql(QueryEngine engine, String sql) {
        List<FieldsQueryCursor<List<?>>> cur = engine.query(null, "PUBLIC", sql);

        try (QueryCursor<List<?>> srvCursor = cur.get(0)) {
            return srvCursor.getAll();
        }
    }

    /** */
    public static List<List<?>> executeSql(IgniteEx ign, String sql) {
        return executeSql(queryProcessor(ign), sql);
    }

    /** */
    public static CalciteQueryProcessor queryProcessor(IgniteEx ign) {
        return Commons.lookupComponent(ign.context(), CalciteQueryProcessor.class);
    }
}
