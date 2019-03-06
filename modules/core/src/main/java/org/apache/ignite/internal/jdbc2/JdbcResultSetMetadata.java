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

package org.apache.ignite.internal.jdbc2;

import java.sql.*;
import java.util.*;

/**
 * JDBC result set metadata implementation.
 */
public class JdbcResultSetMetadata implements ResultSetMetaData {
    /** Column width. */
    private static final int COL_WIDTH = 30;

    /** Table names. */
    private final List<String> tbls;

    /** Column names. */
    private final List<String> cols;

    /** Class names. */
    private final List<String> types;

    /**
     * @param tbls Table names.
     * @param cols Column names.
     * @param types Types.
     */
    JdbcResultSetMetadata(List<String> tbls, List<String> cols, List<String> types) {
        assert cols != null;
        assert types != null;

        this.tbls = tbls;
        this.cols = cols;
        this.types = types;
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() throws SQLException {
        return cols.size();
    }

    /** {@inheritDoc} */
    @Override public boolean isAutoIncrement(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isCaseSensitive(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isSearchable(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isCurrency(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int isNullable(int col) throws SQLException {
        return columnNullable;
    }

    /** {@inheritDoc} */
    @Override public boolean isSigned(int col) throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public int getColumnDisplaySize(int col) throws SQLException {
        return COL_WIDTH;
    }

    /** {@inheritDoc} */
    @Override public String getColumnLabel(int col) throws SQLException {
        return cols.get(col - 1);
    }

    /** {@inheritDoc} */
    @Override public String getColumnName(int col) throws SQLException {
        return cols.get(col - 1);
    }

    /** {@inheritDoc} */
    @Override public String getSchemaName(int col) throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public int getPrecision(int col) throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getScale(int col) throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String getTableName(int col) throws SQLException {
        return tbls != null ? tbls.get(col - 1) : "";
    }

    /** {@inheritDoc} */
    @Override public String getCatalogName(int col) throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public int getColumnType(int col) throws SQLException {
        return JdbcUtils.type(types.get(col - 1));
    }

    /** {@inheritDoc} */
    @Override public String getColumnTypeName(int col) throws SQLException {
        return JdbcUtils.typeName(types.get(col - 1));
    }

    /** {@inheritDoc} */
    @Override public boolean isReadOnly(int col) throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isWritable(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDefinitelyWritable(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String getColumnClassName(int col) throws SQLException {
        return types.get(col - 1);
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("Result set meta data is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface == ResultSetMetaData.class;
    }
}
