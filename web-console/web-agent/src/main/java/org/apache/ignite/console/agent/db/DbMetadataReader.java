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

package org.apache.ignite.console.agent.db;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import org.apache.ignite.console.agent.db.dialect.DB2MetadataDialect;
import org.apache.ignite.console.agent.db.dialect.DatabaseMetadataDialect;
import org.apache.ignite.console.agent.db.dialect.DremioMetadataDialect;
import org.apache.ignite.console.agent.db.dialect.HiveSQLMetadataDialect;
import org.apache.ignite.console.agent.db.dialect.JdbcMetadataDialect;
import org.apache.ignite.console.agent.db.dialect.MySQLMetadataDialect;
import org.apache.ignite.console.agent.db.dialect.OracleMetadataDialect;
import org.apache.ignite.console.agent.db.dialect.PostgreSQLMetadataDialect;
import org.apache.ignite.console.db.DBInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Singleton to extract database metadata.
 */
public class DbMetadataReader {
    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(DbMetadataReader.class.getName());


    /**
     * Get specified dialect object for selected database.
     *
     * @param conn Connection to database.
     * @return Specific dialect object.
     */
    private DatabaseMetadataDialect dialect(Connection conn) {
        try {
            String dbProductName = conn.getMetaData().getDatabaseProductName();

            if ("Oracle".equals(dbProductName))
                return new OracleMetadataDialect(conn);

            if (dbProductName.startsWith("DB2/"))
                return new DB2MetadataDialect(conn);

            if ("MySQL".equals(dbProductName))
                return new MySQLMetadataDialect(conn);
            
            if ("PostgreSQL".equals(dbProductName))
                return new PostgreSQLMetadataDialect(conn);
            
            if (dbProductName.startsWith("Dremio"))
                return new DremioMetadataDialect(conn);
            
            if (dbProductName.contains("Hive"))
                return new HiveSQLMetadataDialect(conn);

            return new JdbcMetadataDialect(conn,Dialect.GENERIC);
        }
        catch (SQLException e) {
            log.error("Failed to resolve dialect (JdbcMetaDataDialect will be used.", e);

            return new JdbcMetadataDialect(conn,Dialect.GENERIC);
        }
    }


    /**
     * Get current of Catalog from database.
     *
     * @param conn Connection to database.
     * @param importSamples If {@code true} include sample schemas.
     * @return List of schema names.
     * @throws SQLException If schemas loading failed.
     */
    public String catalog(Connection conn) throws SQLException  {
    	ResultSet result = conn.getMetaData().getCatalogs();
        if(result.next()){
        	return result.getString(1);
        }
        return null;
    }
    
    /**
     * Get list of schemas from database.
     *
     * @param conn Connection to database.
     * @param importSamples If {@code true} include sample schemas.
     * @return List of schema names.
     * @throws SQLException If schemas loading failed.
     */
    public Collection<String> schemas(Connection conn, boolean importSamples) throws SQLException  {
        return dialect(conn).schemas(conn, importSamples);
    }

    /**
     * Extract DB metadata.
     *
     * @param conn Connection.
     * @param schemas List of database schemas to process. In case of empty list all schemas will be processed.
     * @param tblsOnly Tables only flag.
     */
    public Collection<DbTable> metadata(Connection conn, List<String> schemas, boolean tblsOnly) throws SQLException {
        return dialect(conn).tables(conn, schemas, tblsOnly);
    }
    
    public Collection<DbTable> cachedMetadata(Connection conn, List<String> schemas, boolean tblsOnly) throws SQLException {
        return dialect(conn).cachedTables(conn, schemas, tblsOnly);
    }

    /**
     * Connect to database.
     *
     * @param jdbcDrvJarPath Path to JDBC driver.
     * @param jdbcDrvCls JDBC class name.
     * @param jdbcUrl JDBC connection URL.
     * @param jdbcInfo Connection properties.
     * @return Connection to database.
     * @throws SQLException if connection failed.
     */
    public Connection connect(String jdbcDrvJarPath, DBInfo dbInfo)
        throws SQLException {
        Driver drv = DataSourceManager.drivers.get(dbInfo.getDriverCls());

        if (drv == null && jdbcDrvJarPath!=null) {
        	
            if (jdbcDrvJarPath.isEmpty())
                throw new IllegalStateException("Driver jar file name is not specified.");

            File drvJar = new File(jdbcDrvJarPath);

            if (!drvJar.exists())
                throw new IllegalStateException("Driver jar file is not found.");

            try {
                URL u = new URL("jar:" + drvJar.toURI() + "!/");

                URLClassLoader ucl = URLClassLoader.newInstance(new URL[] {u});

                drv = (Driver)Class.forName(dbInfo.getDriverCls(), true, ucl).newInstance();

                DataSourceManager.drivers.put(dbInfo.getDriverCls(), drv);                
                
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        else if (drv == null && jdbcDrvJarPath==null) {
           
            try {
                drv = (Driver)Class.forName(dbInfo.getDriverCls()).newInstance();

                DataSourceManager.drivers.put(dbInfo.getDriverCls(), drv);
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        Connection conn = drv.connect(dbInfo.jdbcUrl, dbInfo.getJdbcProp());

        if (conn == null)
            throw new IllegalStateException("Connection was not established (JDBC driver returned null value).");        

        return conn;
    }
}
