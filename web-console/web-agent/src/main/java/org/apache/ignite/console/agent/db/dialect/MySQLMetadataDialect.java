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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ignite.console.agent.db.Dialect;


/**
 * MySQL specific metadata dialect. useInformationSchema=true
 */
public class MySQLMetadataDialect extends JdbcMetadataDialect {
    
	public MySQLMetadataDialect(Connection conn) {
		super(conn, Dialect.MYSQL);		
	}

	/** Type name index. */
    private static final int TYPE_NAME_IDX = 1;
    private static final String CONSTRAINT_NAME_PRIMARY_KEY = "PRIMARY";

    /** {@inheritDoc} */
    @Override public Set<String> systemSchemas() {
        return new HashSet<>(Arrays.asList("information_schema", "mysql", "performance_schema", "sys"));
    }

    /** {@inheritDoc} */
    @Override protected ResultSet getSchemas(Connection conn) throws SQLException {
        return conn.getMetaData().getCatalogs();
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
    
    @Override
	protected List<String> getPrimaryKeyDefines(Connection connection, String catalog, String schema, String table) throws SQLException {
		String sql = "select table_name, column_name, ordinal_position from information_schema.key_column_usage where table_schema = ? and constraint_name=? and table_name = ?";
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List<String> primaryKeyList = new ArrayList<>();
		preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setString(1, catalog);
		preparedStatement.setString(2, CONSTRAINT_NAME_PRIMARY_KEY);
		preparedStatement.setString(3, table);
		resultSet = preparedStatement.executeQuery();

		String primaryKey = null;			
		String tableName = null;
		while (resultSet.next()) {
			tableName = resultSet.getString("table_name");
			primaryKey = resultSet.getString("column_name");
			primaryKeyList.add(primaryKey);
		}
		resultSet.close();
		preparedStatement.close();
		return primaryKeyList;
	}
}
