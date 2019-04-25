/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.jdbc.thin;

import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Qualified sql query: schema + query itself.
 */
public final class QualifiedSQLQuery {
    /** Schema name. */
    private final String schemaName;

    /** Sql Query. */
    private final String sqlQry;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param sqlQry Sql Query.
     */
    public QualifiedSQLQuery(String schemaName, String sqlQry) {
        this.schemaName = schemaName;
        this.sqlQry = sqlQry;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Sql query.
     */
    public String sqlQuery() {
        return sqlQry;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QualifiedSQLQuery qry = (QualifiedSQLQuery)o;

        return Objects.equals(schemaName, qry.schemaName) &&
            Objects.equals(sqlQry, qry.sqlQry);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = schemaName != null ? schemaName.hashCode() : 0;

        res = 31 * res + (sqlQry != null ? sqlQry.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QualifiedSQLQuery.class, this);
    }
}
