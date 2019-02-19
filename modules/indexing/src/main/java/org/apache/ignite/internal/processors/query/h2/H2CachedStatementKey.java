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

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlanBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * H2 cached statement key.
 */
class H2CachedStatementKey {
    /** Schema name. */
    private final String schemaName;

    /** SQL. */
    private final String sql;

    /** Flags. */
    private final byte flags;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param sql SQL.
     */
    public H2CachedStatementKey(String schemaName, String sql) {
        this(schemaName, sql, null);
    }

    /**
     * Full-fledged constructor.
     *
     * @param schemaName Schema name.
     * @param sql SQL.
     * @param fieldsQry Query with flags.
     */
    public H2CachedStatementKey(String schemaName, String sql, SqlFieldsQuery fieldsQry) {
        this.schemaName = schemaName;
        this.sql = sql;

        if (fieldsQry == null)
            this.flags = 0; // flags only relevant for server side updates.
        else {
            this.flags = (byte)(1 +
                (fieldsQry.isDistributedJoins() ? 2 : 0) +
                (fieldsQry.isEnforceJoinOrder() ? 4 : 0) +
                (fieldsQry.isCollocated() ? 8 : 0) +
                (fieldsQry.isLocal() ? 8 : 0)
            );
        }
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (31 * (schemaName != null ? schemaName.hashCode() : 0) + (sql != null ? sql.hashCode() : 0)) +
            flags;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        H2CachedStatementKey other = (H2CachedStatementKey)o;

        return F.eq(sql, other.sql) && F.eq(schemaName, other.schemaName) && flags == other.flags;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2CachedStatementKey.class, this);
    }
}
