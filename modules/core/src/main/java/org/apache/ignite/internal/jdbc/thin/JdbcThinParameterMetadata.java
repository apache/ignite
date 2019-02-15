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
    @SuppressWarnings("unchecked")
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