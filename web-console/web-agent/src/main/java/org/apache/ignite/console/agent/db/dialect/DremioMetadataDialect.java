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
