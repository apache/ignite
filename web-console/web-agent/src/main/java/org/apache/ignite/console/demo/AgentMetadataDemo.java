

package org.apache.ignite.console.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.ignite.console.agent.db.DBInfo;
import org.apache.ignite.console.agent.db.DataSourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import static org.apache.ignite.console.agent.AgentUtils.resolvePath;

/**
 * Demo for metadata load from database.
 *
 * H2 database will be started and several tables will be created.
 */
public class AgentMetadataDemo {
    /** */
    private static final Logger log = LoggerFactory.getLogger(AgentMetadataDemo.class.getName());
    /** */
    private static final String DEMO_SCRIPT_PATH = "demo/db-init.sql";

    private static Connection conn = null;

    /**
     * @param jdbcUrl Connection URL.
     * @return {@code true} if URL is used for test-drive.
     */
    public static boolean isTestDriveUrl(String jdbcUrl) {
        return "jdbc:h2:mem:demo-db".equals(jdbcUrl);
    }

    public static DataSource bindTestDatasource() {
        String jndi = "dsH2";
        try {
            testDrive();
            return DataSourceManager.getDataSource(jndi);
        } catch (SQLException e) {
            DBInfo demoDB = new DBInfo();
            demoDB.setDb("demo-db");
            demoDB.setJdbcUrl("jdbc:h2:mem:demo-db");
            demoDB.setUserName("sa");
            demoDB.setPassword("");
            return DataSourceManager.bindDataSource(jndi,demoDB);
        }
    }

    /**
     * Start H2 database and populate it with several tables.
     */
    public static synchronized Connection testDrive() throws SQLException {
        if (conn==null || conn.isClosed()) {
            log.info("DEMO: Prepare in-memory H2 database...");

            try {
                Driver driver = null;
                try {
                    Class driverCls = Class.forName("org.h2.Driver");
                    driver = (Driver)driverCls.getDeclaredConstructor().newInstance();
                }catch (Exception e){
                    Class driverCls = Class.forName("shared.org.h2.Driver");
                    driver = (Driver)driverCls.getDeclaredConstructor().newInstance();
                }

                conn = driver.connect("jdbc:h2:mem:demo-db;DB_CLOSE_DELAY=-1",null);

                File sqlScript = resolvePath(DEMO_SCRIPT_PATH);

                if (sqlScript == null)
                    throw new FileNotFoundException(DEMO_SCRIPT_PATH);

                // RunScript.execute(conn, new FileReader(sqlScript));
                FileReader reader = new FileReader(sqlScript);

                try {
                    Class<?> runScriptClass = Class.forName("org.h2.tools.RunScript");
                    Method executeMethod = runScriptClass.getMethod(
                            "execute", Connection.class, Reader.class);
                    executeMethod.invoke(null, conn, reader);   // 静态方法，第一个参数传 null
                }
                catch (Exception e){
                    try {
                        Class<?> runScriptClass = Class.forName("shared.org.h2.tools.RunScript");
                        Method executeMethod = runScriptClass.getMethod(
                                "execute", Connection.class, Reader.class);
                        executeMethod.invoke(null, conn, reader);   // 静态方法，第一个参数传 null
                    }
                    catch (ClassNotFoundException e2){
                        log.error("DEMO: Failed to load H2 driver!", e);

                        throw new SQLException("Failed to load H2 driver", e);
                    }
                    catch (Exception e2){
                        log.error("DEMO: Failed to start test drive for metadata!", e);
                        throw new SQLException("DEMO: Failed to start test drive for metadata!", e);
                    }
                }

                log.info("DEMO: Sample tables created.");

                log.info("DEMO: JDBC URL for test drive metadata load: jdbc:h2:mem:demo-db");

            }
            catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                   InvocationTargetException e) {
                log.error("DEMO: Failed to load H2 driver!", e);

                throw new SQLException("Failed to load H2 driver", e);
            }
            catch (SQLException e) {
                log.error("DEMO: Failed to start test drive for metadata!", e);

                throw e;
            }
            catch (FileNotFoundException e) {
                log.error("DEMO: Failed to find demo database initialization script file: " + DEMO_SCRIPT_PATH);

                throw new SQLException("Failed to start demo for metadata", e);
            }
        }
       
        return conn;
    }
}
