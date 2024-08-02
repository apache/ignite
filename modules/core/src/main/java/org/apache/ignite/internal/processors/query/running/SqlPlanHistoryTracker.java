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

package org.apache.ignite.internal.processors.query.running;

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;

/** Class that manages recording and storing SQL plans. */
public class SqlPlanHistoryTracker {
    /** SQL plan history. */
    private final GridBoundedConcurrentLinkedHashMap<SqlPlanKey, SqlPlan> sqlPlanHistory;

    /** SQL plan history size. */
    private final int historySize;

    /**
     * @param historySize SQL plan history size.
     */
    public SqlPlanHistoryTracker(int historySize) {
        this.historySize = historySize;

        sqlPlanHistory = (historySize > 0) ? new GridBoundedConcurrentLinkedHashMap<>(historySize) : null;
    }

    /**
     * @param plan SQL plan.
     * @param qry Query.
     * @param schema Schema name.
     * @param loc Local query flag.
     * @param startTs Start query timestamp.
     * @param engine SQL engine.
     */
    public void addPlan(String plan, String qry, String schema, boolean loc, long startTs, SqlEngine engine) {
        if (historySize <= 0)
            return;

        SqlPlan sqlPlan = new SqlPlan(plan, qry, schema, loc, startTs, engine);

        sqlPlanHistory.put(sqlPlan.key(), sqlPlan);
    }

    /** */
    public Map<SqlPlanKey, SqlPlan> sqlPlanHistory() {
        if (historySize <= 0)
            return Collections.emptyMap();

        return Collections.unmodifiableMap(sqlPlanHistory);
    }

    /** */
    public enum SqlEngine {
        /** */
        CALCITE,

        /** */
        H2
    }
}
