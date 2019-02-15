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

package org.apache.ignite.console.agent.db.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * MySQL specific metadata dialect.
 */
public class MySQLMetadataDialect extends JdbcMetadataDialect {
    /** Type name index. */
    private static final int TYPE_NAME_IDX = 1;

    /** {@inheritDoc} */
    @Override public Set<String> systemSchemas() {
        return new HashSet<>(Arrays.asList("information_schema", "mysql", "performance_schema", "sys"));
    }

    /** {@inheritDoc} */
    @Override public Collection<String> schemas(Connection conn) throws SQLException {
        Collection<String> schemas = new ArrayList<>();

        ResultSet rs = conn.getMetaData().getCatalogs();

        Set<String> sys = systemSchemas();

        while(rs.next()) {
            String schema = rs.getString(1);

            // Skip system schemas.
            if (sys.contains(schema))
                continue;

            schemas.add(schema);
        }

        return schemas;
    }

    /** {@inheritDoc} */
    @Override protected boolean useCatalog() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean useSchema() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Set<String> unsignedTypes(DatabaseMetaData dbMeta) throws SQLException {
        Set<String> unsignedTypes = new HashSet<>();

        try (ResultSet typeRs = dbMeta.getTypeInfo()) {
            while (typeRs.next()) {
                String typeName = typeRs.getString(TYPE_NAME_IDX);

                if (typeName.contains("UNSIGNED"))
                    unsignedTypes.add(typeName);
            }
        }

        return unsignedTypes;
    }
}
