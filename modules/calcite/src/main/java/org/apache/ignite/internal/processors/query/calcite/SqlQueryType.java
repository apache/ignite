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

package org.apache.ignite.internal.processors.query.calcite;

import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlan;

/**
 * Possible query types.
 */
public enum SqlQueryType {
    /** Query. */
    QUERY,

    /** DML. */
    DML,

    /** DDL. */
    DDL,

    /** Explain. */
    EXPLAIN;

    /**
     * @param type QueryPlan.Type.
     * @return Associated SqlQueryType.
     */
    public static SqlQueryType mapPlanTypeToSqlType(QueryPlan.Type type) {
        switch (type) {
            case QUERY:
                return QUERY;
            case DML:
                return DML;
            case DDL:
                return DDL;
            case EXPLAIN:
                return EXPLAIN;
            default:
                throw new UnsupportedOperationException("Unexpected query plan type: " + type.name());
        }
    }
}
