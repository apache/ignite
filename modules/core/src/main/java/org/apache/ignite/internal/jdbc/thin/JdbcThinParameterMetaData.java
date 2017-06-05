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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcParamMeta;

/**
 * JDBC prepared statement implementation.
 */
public class JdbcThinParameterMetaData implements ParameterMetaData {
    /** Meta. */
    private final List<JdbcParamMeta> meta;

    /**
     * @param meta Parameters metadata.
     */
    public JdbcThinParameterMetaData(List<JdbcParamMeta> meta) {
        this.meta = meta;
    }

    /** {@inheritDoc} */
    @Override public int getParameterCount() throws SQLException {
        return meta.size();
    }

    /** {@inheritDoc} */
    @Override public int isNullable(int param) throws SQLException {
        return meta.get(param -1).isNullable();
    }

    /** {@inheritDoc} */
    @Override public boolean isSigned(int param) throws SQLException {
        return meta.get(param -1).isSigned();
    }

    /** {@inheritDoc} */
    @Override public int getPrecision(int param) throws SQLException {
        return meta.get(param -1).precision();
    }

    /** {@inheritDoc} */
    @Override public int getScale(int param) throws SQLException {
        return meta.get(param -1).scale();
    }

    /** {@inheritDoc} */
    @Override public int getParameterType(int param) throws SQLException {
        return meta.get(param -1).type();
    }

    /** {@inheritDoc} */
    @Override public String getParameterTypeName(int param) throws SQLException {
        return meta.get(param -1).typeName();
    }

    /** {@inheritDoc} */
    @Override public String getParameterClassName(int param) throws SQLException {
        return meta.get(param -1).typeClass();
    }

    /** {@inheritDoc} */
    @Override public int getParameterMode(int param) throws SQLException {
        return meta.get(param -1).mode();
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("Parameters metadata is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcThinParameterMetaData.class);
    }
}