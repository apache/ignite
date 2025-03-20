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

package org.apache.ignite.console.agent.db.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.ignite.console.agent.db.Dialect;

/**
 * DB2 specific metadata dialect.
 */
public class PostgreSQLMetadataDialect extends JdbcMetadataDialect {

	/** Type name index. */
    private static final int TYPE_NAME_IDX = 1;
	
	public PostgreSQLMetadataDialect(Connection conn) {
		super(conn, Dialect.POSTGRESQL);		
	}
	
    /** {@inheritDoc} */
    @Override public Set<String> systemSchemas() {
        return new HashSet<>(Arrays.asList("pg_catalog", "information_schema"));
    }
    
    @Override public Set<String> unsignedTypes(DatabaseMetaData dbMeta) throws SQLException {
        Set<String> unsignedTypes = new HashSet<>();

        try (ResultSet typeRs = dbMeta.getTypeInfo()) {
            while (typeRs.next()) {
                String typeName = typeRs.getString(TYPE_NAME_IDX);

                if (typeName.toLowerCase().contains("unsigned"))
                    unsignedTypes.add(typeName);
            }
        }

        return unsignedTypes;
    }
    
}
