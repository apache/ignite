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

package org.apache.ignite.schema.parser;

import javafx.collections.*;
import org.apache.ignite.schema.model.*;
import org.apache.ignite.schema.parser.dialect.*;

import java.sql.*;
import java.util.*;
import java.util.logging.*;

/**
 * Database metadata parser.
 */
public class DatabaseMetadataParser {
    /** Logger. */
    private static final Logger log = Logger.getLogger(DatabaseMetadataParser.class.getName());

    /**
     * Get specified dialect object for selected database.
     *
     * @param conn Connection to database.
     * @return Specific dialect object.
     */
    private static DatabaseMetadataDialect dialect(Connection conn) {
        try {
            String dbProductName = conn.getMetaData().getDatabaseProductName();

            if ("Oracle".equals(dbProductName))
                return new OracleMetadataDialect();
            else if (dbProductName.startsWith("DB2/"))
                return new DB2MetadataDialect();
            else if (dbProductName.equals("MySQL"))
                return new MySQLMetadataDialect();
            else
                return new JdbcMetadataDialect();
        }
        catch (SQLException e) {
            log.log(Level.SEVERE, "Failed to resolve dialect (JdbcMetaDataDialect will be used.", e);

            return new JdbcMetadataDialect();
        }
    }

    /**
     * Get list of schemas from database.
     *
     * @param conn Connection to database.
     * @return List of schema descriptors.
     * @throws SQLException If shemas loading failed.
     */
    public static ObservableList<SchemaDescriptor> schemas(Connection conn) throws SQLException  {
        List<String> dbSchemas = dialect(conn).schemas(conn);

        List<SchemaDescriptor> uiSchemas = new ArrayList<>(dbSchemas.size());

        for (String schema : dbSchemas)
            uiSchemas.add(new SchemaDescriptor(schema, false));

        return FXCollections.observableList(uiSchemas);
    }

    /**
     * Parse database metadata.
     *
     * @param conn Connection to database.
     * @param schemas Collention of schema names to load.
     * @param tblsOnly If {@code true} then process tables only else process tables and views.
     * @return Collection of POJO descriptors.
     * @throws SQLException If parsing failed.
     */
    public static ObservableList<PojoDescriptor> parse(Connection conn, List<String> schemas, boolean tblsOnly)
        throws SQLException {
        DatabaseMetadataDialect dialect = dialect(conn);

        Map<String, PojoDescriptor> parents = new HashMap<>();

        Map<String, Collection<PojoDescriptor>> childrens = new HashMap<>();

        for (DbTable tbl : dialect.tables(conn, schemas, tblsOnly)) {
            String schema = tbl.schema();

            PojoDescriptor parent = parents.get(schema);
            Collection<PojoDescriptor> children = childrens.get(schema);

            if (parent == null) {
                parent = new PojoDescriptor(null, new DbTable(schema, "", Collections.<DbColumn>emptyList(),
                    Collections.<String>emptySet(), Collections.<String>emptySet(),
                    Collections.<String, Map<String, Boolean>>emptyMap()));

                children = new ArrayList<>();

                parents.put(schema, parent);
                childrens.put(schema, children);
            }

            children.add(new PojoDescriptor(parent, tbl));
        }

        List<PojoDescriptor> res = new ArrayList<>();

        for (String schema : parents.keySet()) {
            PojoDescriptor parent = parents.get(schema);
            Collection<PojoDescriptor> children = childrens.get(schema);

            if (!children.isEmpty()) {
                parent.children(children);

                res.add(parent);
                res.addAll(children);
            }
        }

        Collections.sort(res, new Comparator<PojoDescriptor>() {
            @Override public int compare(PojoDescriptor o1, PojoDescriptor o2) {
                return o1.fullDbName().compareTo(o2.fullDbName());
            }
        });

        return FXCollections.observableList(res);
    }
}
