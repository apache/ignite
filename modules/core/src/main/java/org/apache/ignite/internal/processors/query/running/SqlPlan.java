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

import java.util.Objects;

/** Representation of an entry in SQL plan history. */
public class SqlPlan {
    /** SQL plan. */
    private final String plan;

    /** Query. */
    private final String qry;

    /** Schema name. */
    private final String schema;

    /** Local query flag. */
    private final boolean loc;

    /** SQL engine. */
    private final String engine;

    /** Pre-calculated hash code. */
    private final int hash;

    /**
     * @param plan SQL plan.
     * @param qry Query.
     * @param schema Schema name.
     * @param loc Local query flag.
     * @param engine SQL engine.
     */
    public SqlPlan(
        String plan,
        String qry,
        String schema,
        boolean loc,
        String engine
    ) {
        this.plan = plan;
        this.qry = qry;
        this.schema = schema;
        this.loc = loc;
        this.engine = engine;

        hash = Objects.hash(plan, qry, schema, loc, engine);
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

    /** */
    public String engine() {
        return engine;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return hash;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SqlPlan plan0 = (SqlPlan)o;

        return Objects.equals(plan, plan0.plan) && Objects.equals(qry, plan0.qry) && Objects.equals(schema, plan0.schema) && Objects.equals(loc, plan0.loc)
            && Objects.equals(engine, plan0.engine);
    }
}
