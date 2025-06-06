

package org.apache.ignite.console.agent.db.dialect;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.ignite.console.agent.db.Dialect;


/**
 * DB2 specific metadata dialect.
 */
public class DB2MetadataDialect extends JdbcMetadataDialect {
	
	public DB2MetadataDialect(Connection conn) {
		super(conn, Dialect.DB2);		
	}
	
    /** {@inheritDoc} */
    @Override public Set<String> systemSchemas() {
        return new HashSet<>(Arrays.asList("SYSIBM", "SYSCAT", "SYSSTAT", "SYSTOOLS", "SYSFUN", "SYSIBMADM",
            "SYSIBMINTERNAL", "SYSIBMTS", "SYSPROC", "SYSPUBLIC"));
    }
}
