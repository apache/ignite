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
public class QueryDescriptor {
    /** */
    private final String schemaName;

    /** */
    private final String sql;

    /** */
    private final boolean collocated;

    /** */
    private final boolean distributedJoins;

    /** */
    private final boolean enforceJoinOrder;

    /** */
    private final boolean loc;

    /** Skip reducer on update flag. */
    private final boolean skipReducerOnUpdate;

    /** Batched flag. */
    private final boolean batched;

    /**
     * @param schemaName Schema name.
     * @param sql Sql.
     * @param collocated Collocated GROUP BY.
     * @param distributedJoins Distributed joins enabled.
     * @param enforceJoinOrder Enforce join order of tables.
     * @param loc Query is local flag.
     * @param skipReducerOnUpdate Skip reducer on update flag.
     */
    QueryDescriptor(
        String schemaName,
        String sql,
        boolean collocated,
        boolean distributedJoins,
        boolean enforceJoinOrder,
        boolean loc,
        boolean skipReducerOnUpdate,
        boolean batched
    ) {
        this.schemaName = schemaName;
        this.sql = sql;
        this.collocated = collocated;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder = enforceJoinOrder;
        this.loc = loc;
        this.skipReducerOnUpdate = skipReducerOnUpdate;
        this.batched = batched;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return SQL.
     */
    public String sql() {
        return sql;
    }

    /**
     * @return Collocated GROUP BY flag.
     */
    public boolean collocated() {
        return collocated;
    }

    /**
     * @return Distributed joins flag.
     */
    public boolean distributedJoins() {
        return distributedJoins;
    }

    /**
     * @return Enforce join order flag.
     */
    public boolean enforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * @return Local flag.
     */
    public boolean local() {
        return loc;
    }

    /**
     * @return Skip reducer on update flag.
     */
    public boolean skipReducerOnUpdate() {
        return skipReducerOnUpdate;
    }

    /**
     * @return Batched flag.
     */
    public boolean batched() {
        return batched;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SimplifiableIfStatement")
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryDescriptor that = (QueryDescriptor)o;

        if (collocated != that.collocated)
            return false;

        if (distributedJoins != that.distributedJoins)
            return false;

        if (enforceJoinOrder != that.enforceJoinOrder)
            return false;

        if (skipReducerOnUpdate != that.skipReducerOnUpdate)
            return false;

        if (batched != that.batched)
            return false;

        if (schemaName != null ? !schemaName.equals(that.schemaName) : that.schemaName != null)
            return false;

        return loc == that.loc && sql.equals(that.sql);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("AssignmentReplaceableWithOperatorAssignment")
    @Override public int hashCode() {
        int res = schemaName != null ? schemaName.hashCode() : 0;

        res = 31 * res + sql.hashCode();
        res = 31 * res + (collocated ? 1 : 0);

        res = res + (distributedJoins ? 2 : 0);
        res = res + (enforceJoinOrder ? 4 : 0);
        res = res + (loc ? 8 : 0);
        res = res + (skipReducerOnUpdate ? 16 : 0);
        res = res + (batched ? 32 : 0);

        return res;
    }
}
