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

package org.apache.ignite.schema.parser.dialect;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * MySQL specific metadata dialect.
 */
public class MySQLMetadataDialect extends JdbcMetadataDialect {
    /** {@inheritDoc} */
    @Override public Collection<String> schemas(Connection conn) throws SQLException {
        List<String> schemas = new ArrayList<>();

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
}
