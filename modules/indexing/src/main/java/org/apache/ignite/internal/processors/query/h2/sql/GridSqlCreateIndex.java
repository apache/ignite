/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.Map;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.h2.command.Parser;

/**
 * CREATE INDEX statement.
 */
public class GridSqlCreateIndex extends GridSqlStatement {
    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Attempt to create the index only if it does not exist. */
    private boolean ifNotExists;

    /** Index to create. */
    private QueryIndex idx;

    /**
     * @return Schema name for new index.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name for new index.
     */
    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @param tblName Table name.
     */
    public void tableName(String tblName) {
        this.tblName = tblName;
    }

    /**
     * @return whether attempt to create the index should be made only if it does not exist.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * @param ifNotExists whether attempt to create the index should be made only if it does not exist.
     */
    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    /**
     * @return Index to create.
     */
    public QueryIndex index() {
        return idx;
    }

    /**
     * @param idx Index to create.
     */
    public void index(QueryIndex idx) {
        this.idx = idx;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StringBuilder sb = new StringBuilder("CREATE ")
            .append(idx.getIndexType() == QueryIndexType.GEOSPATIAL ? "SPATIAL " : "")
            .append("INDEX ").append(ifNotExists ? "IF NOT EXISTS " : "")
            .append(Parser.quoteIdentifier(schemaName)).append('.')
            .append(Parser.quoteIdentifier(idx.getName())).append(" ON ")
            .append(Parser.quoteIdentifier(tblName)).append(" (");

        boolean first = true;

        for (Map.Entry<String, Boolean> e : idx.getFields().entrySet()) {
            if (first)
                first = false;
            else
                sb.append(", ");

            sb.append(Parser.quoteIdentifier(e.getKey())).append(e.getValue() ? " ASC" : " DESC");
        }

        sb.append(')');

        return sb.toString();
    }
}
