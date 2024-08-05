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

/** Representation of an entry in SQL plan history. */
public class SqlPlan {
    /** */
    private final SqlPlanKey key;

    /** */
    private final SqlPlanValue val;

    /**
     * @param plan SQL plan.
     * @param qry Query.
     * @param schema Schema name.
     * @param loc Local query flag.
     * @param startTime Start query timestamp.
     */
    public SqlPlan(
        String plan,
        String qry,
        String schema,
        boolean loc,
        long startTime,
        SqlPlanHistoryTracker.SqlEngine engine
    ) {
        key = new SqlPlanKey(plan, qry, schema, loc);

        val = new SqlPlanValue(startTime, engine);
    }

    /** */
    public SqlPlanKey key() {
        return key;
    }

    /** */
    public SqlPlanValue value() {
        return val;
    }

    /** */
    public String plan() {
        return key.plan();
    }

    /** */
    public String query() {
        return key.query();
    }

    /** */
    public String schema() {
        return key.schema();
    }

    /** */
    public boolean local() {
        return key.local();
    }

    /** */
    public long startTime() {
        return val.startTime();
    }

    /** */
    public String engine() {
        return val.engine();
    }
}
