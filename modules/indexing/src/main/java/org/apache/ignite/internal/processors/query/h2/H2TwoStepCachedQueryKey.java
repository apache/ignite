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

package org.apache.ignite.internal.processors.query.h2;

/**
 * Key for cached two-step query.
 */
public class H2TwoStepCachedQueryKey {
    /** */
    private final String schemaName;

    /** */
    private final String sql;

    /** */
    private final boolean grpByCollocated;

    /** */
    private final boolean distributedJoins;

    /** */
    private final boolean enforceJoinOrder;

    /** */
    private final boolean isLocal;

    /**
     * @param schemaName Schema name.
     * @param sql Sql.
     * @param grpByCollocated Collocated GROUP BY.
     * @param distributedJoins Distributed joins enabled.
     * @param enforceJoinOrder Enforce join order of tables.
     * @param isLocal Query is local flag.
     */
    H2TwoStepCachedQueryKey(String schemaName,
        String sql,
        boolean grpByCollocated,
        boolean distributedJoins,
        boolean enforceJoinOrder,
        boolean isLocal) {
        this.schemaName = schemaName;
        this.sql = sql;
        this.grpByCollocated = grpByCollocated;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder = enforceJoinOrder;
        this.isLocal = isLocal;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        H2TwoStepCachedQueryKey that = (H2TwoStepCachedQueryKey)o;

        if (grpByCollocated != that.grpByCollocated)
            return false;

        if (distributedJoins != that.distributedJoins)
            return false;

        if (enforceJoinOrder != that.enforceJoinOrder)
            return false;

        if (schemaName != null ? !schemaName.equals(that.schemaName) : that.schemaName != null)
            return false;

        return isLocal == that.isLocal && sql.equals(that.sql);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = schemaName != null ? schemaName.hashCode() : 0;
        res = 31 * res + sql.hashCode();
        res = 31 * res + (grpByCollocated ? 1 : 0);
        res = res + (distributedJoins ? 2 : 0);
        res = res + (enforceJoinOrder ? 4 : 0);
        res = res + (isLocal ? 8 : 0);

        return res;
    }
}
