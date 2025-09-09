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
import org.apache.ignite.internal.util.typedef.internal.U;

/** Class that manages recording and storing SQL plans. */
public class SqlPlanHistoryTracker {
    /** Empty map. */
    private static final Map<SqlPlan, Long> EMPTY_MAP = Collections.emptyMap();

    /** SQL plan history. */
    private Map<SqlPlan, Long> sqlPlanHistory;

    /**
     * @param histSize SQL plan history size.
     */
    public SqlPlanHistoryTracker(int histSize) {
        setHistorySize(histSize);
    }

    /**
     * @param plan SQL plan.
     * @param qry Query.
     * @param schema Schema name.
     * @param loc Local query flag.
     * @param engine SQL engine.
     */
    public void addPlan(String plan, String qry, String schema, boolean loc, String engine) {
        if (sqlPlanHistory == EMPTY_MAP || plan.isEmpty())
            return;

        SqlPlan sqlPlan = new SqlPlan(plan, qry, schema, loc, engine);

        sqlPlanHistory.put(sqlPlan, U.currentTimeMillis());
    }

    /** */
    public Map<SqlPlan, Long> sqlPlanHistory() {
        return Collections.unmodifiableMap(sqlPlanHistory);
    }

    /** */
    public boolean enabled() {
        return sqlPlanHistory != EMPTY_MAP;
    }

    /**
     * @param histSize History size.
     */
    public void setHistorySize(int histSize) {
        sqlPlanHistory = (histSize > 0) ? new GridBoundedConcurrentLinkedHashMap<>(histSize) : Collections.emptyMap();
    }
}
