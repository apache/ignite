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

package org.apache.ignite.internal.jdbc.thin;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcColumnMeta;

/**
 * JDBC result set metadata implementation.
 */
public class JdbcThinResultSetMetadata implements ResultSetMetaData {
    /** Column width. */
    private static final int COL_WIDTH = 30;

    /** Table names. */
    private final List<JdbcColumnMeta> meta;

    /**
     * @param meta Metadata.
     */
    JdbcThinResultSetMetadata(List<JdbcColumnMeta> meta) {
        assert meta != null;

        this.meta = meta;
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() throws SQLException {
        return meta.size();
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
        return meta.get(col - 1).columnName();
    }

    /** {@inheritDoc} */
    @Override public String getColumnName(int col) throws SQLException {
        return meta.get(col - 1).columnName();
    }

    /** {@inheritDoc} */
    @Override public String getSchemaName(int col) throws SQLException {
        return meta.get(col - 1).schemaName();
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
        return meta.get(col - 1).tableName();
    }

    /** {@inheritDoc} */
    @Override public String getCatalogName(int col) throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public int getColumnType(int col) throws SQLException {
        return meta.get(col - 1).dataType();
    }

    /** {@inheritDoc} */
    @Override public String getColumnTypeName(int col) throws SQLException {
        return meta.get(col - 1).dataTypeName();
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
        return meta.get(col - 1).dataTypeClass();
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("Result set meta data is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcThinResultSetMetadata.class);
    }
}
