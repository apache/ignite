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

package org.apache.ignite.spi.systemview.view;

import java.util.Date;
import java.util.Map;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.running.SqlPlan;

/** */
public class SqlPlanHistoryView {
    /** SQL plan. */
    private final Map.Entry<SqlPlan, Long> plan;

    /**
     * @param plan SQL plan.
     */
    public SqlPlanHistoryView(Map.Entry<SqlPlan, Long> plan) {
        this.plan = plan;
    }

    /**
     * @return String Schema name.
     */
    @Order
    public String schemaName() {
        return plan.getKey().schema();
    }

    /**
     * @return String SQL query.
     */
    @Order(1)
    public String sql() {
        return plan.getKey().query();
    }

    /**
     * @return String SQL plan.
     */
    @Order(2)
    public String plan() {
        return plan.getKey().plan();
    }

    /**
     * @return boolean Local query flag.
     */
    @Order(3)
    public boolean local() {
        return plan.getKey().local();
    }

    /**
     * @return String SQL engine.
     */
    @Order(4)
    public String engine() {
        return plan.getKey().engine();
    }

    /**
     * @return Date Last time the query was executed.
     */
    @Order(5)
    public Date lastStartTime() {
        return new Date(plan.getValue());
    }
}
