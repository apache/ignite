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

import java.nio.file.Path;
import java.util.List;

/**
 * Tests ensures a planner generates optimal plan for TPC-H queries.
 *
 * @code org.apache.ignite.internal.sql.engine.benchmarks.TpchParseBenchmark
 */
public class TpchQueryPlannerTest extends AbstractTpcQueryPlannerTest {
    /** {@inheritDoc} */
    @Override public String queryString(String queryId) {
        return TpchHelper.getQuery(queryId);
    }

    /** {@inheritDoc} */
    @Override public void updateQueryPlan(String queryId, List<String> newPlans) {
        Path targetDirectory = Path.of("./src/test/resources/tpch/plan");
        updateQueryPlan(queryId, targetDirectory, newPlans);
    }

    /** {@inheritDoc} */
    @Override String name() {
        return "tpch";
    }

    /** {@inheritDoc} */
    @Override Class<? extends Enum<? extends TpcTable>> tables() {
        return TpchTables.class;
    }
}
