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

import org.apache.ignite.internal.util.typedef.F;

/** SQL plan history entry key. */
public class SqlPlanKey {
    /** Plan. */
    private final String plan;

    /** Query. */
    private final String qry;

    /** Schema name. */
    private final String schema;

    /** Local query flag. */
    private final boolean loc;

    /** Pre-calculated hash code. */
    private final int hash;

    /**
     * @param plan SQL Plan.
     * @param qry Query.
     * @param schema Schema name.
     * @param loc Local query flag.
     */
    public SqlPlanKey(String plan, String qry, String schema, boolean loc) {
        assert plan != null;
        assert qry != null;
        assert schema != null;

        this.plan = plan;
        this.qry = qry;
        this.schema = schema;
        this.loc = loc;

        hash = 31 * (31 * plan.hashCode() + qry.hashCode() + schema.hashCode()) + (loc ? 1 : 0);
    }

    /** */
    public String plan() {
        return plan;
    }

    /** */
    public String query() {
        return qry;
    }

    /** */
    public String schema() {
        return schema;
    }

    /** */
    public boolean local() {
        return loc;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SqlPlanKey key = (SqlPlanKey)o;

        return F.eq(plan, key.plan) && F.eq(qry, key.qry) && F.eq(schema, key.schema) && F.eq(loc, key.loc);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return hash;
    }
}
