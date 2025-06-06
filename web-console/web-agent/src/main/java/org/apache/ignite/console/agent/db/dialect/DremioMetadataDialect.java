

package org.apache.ignite.console.agent.db.dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.ignite.console.agent.db.DbTable;
import org.apache.ignite.console.agent.db.Dialect;

/**
 * MySQL specific metadata dialect.
 */
public class DremioMetadataDialect extends JdbcMetadataDialect {
    /** Type name index. */
    private static final int TYPE_NAME_IDX = 1;
    
    public DremioMetadataDialect(Connection conn) {
		super(conn, Dialect.DREMIO);		
	}

    /** {@inheritDoc} */
    @Override public Set<String> systemSchemas() {
        return new HashSet<>(Arrays.asList("information_schema", "performance_schema", "sys"));
    }


    /** {@inheritDoc} */
    @Override protected boolean useCatalog() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean useSchema() {
        return true;
    }


    /** {@inheritDoc} */
    @Override public Collection<DbTable> tables(Connection conn, List<String> schemas, boolean tblsOnly)
        throws SQLException {
    	return tables(conn,schemas,"%", tblsOnly? VIEW_ONLY:VIEW_ONLY);
    }
}
