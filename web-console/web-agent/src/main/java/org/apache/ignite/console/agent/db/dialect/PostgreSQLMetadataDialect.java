

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
