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
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcParameterMeta;

/**
 * JDBC SQL query's parameters metadata.
 */
public class JdbcThinParameterMetadata implements ParameterMetaData {
    /** Parameters metadata. */
    private final List<JdbcParameterMeta> meta;

    /**
     * @param meta Parameters metadata.
     */
    public JdbcThinParameterMetadata(List<JdbcParameterMeta> meta) {
        assert meta != null;

        this.meta = meta;
    }

    /** {@inheritDoc} */
    @Override public int getParameterCount() throws SQLException {
        return meta.size();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("MagicConstant")
    @Override public int isNullable(int param) throws SQLException {
        return parameter(param).isNullable();
    }

    /** {@inheritDoc} */
    @Override public boolean isSigned(int param) throws SQLException {
        return parameter(param).isSigned();
    }

    /** {@inheritDoc} */
    @Override public int getPrecision(int param) throws SQLException {
        return parameter(param).precision();
    }

    /** {@inheritDoc} */
    @Override public int getScale(int param) throws SQLException {
        return parameter(param).scale();
    }

    /** {@inheritDoc} */
    @Override public int getParameterType(int param) throws SQLException {
        return parameter(param).type();
    }

    /** {@inheritDoc} */
    @Override public String getParameterTypeName(int param) throws SQLException {
        return parameter(param).typeName();
    }

    /** {@inheritDoc} */
    @Override public String getParameterClassName(int param) throws SQLException {
        return parameter(param).typeClass();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("MagicConstant")
    @Override public int getParameterMode(int param) throws SQLException {
        return parameter(param).mode();
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("Parameters metadata is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcThinParameterMetadata.class);
    }

    /**
     * Bounds checks the parameter index.
     *
     * @param param Parameter index.
     * @return Parameter.
     * @throws SQLException If failed.
     */
    private JdbcParameterMeta parameter(int param) throws SQLException {
        if (param <= 0 || param > meta.size())
            throw new SQLException("Invalid parameter number");

        return meta.get(param - 1);
    }
}
