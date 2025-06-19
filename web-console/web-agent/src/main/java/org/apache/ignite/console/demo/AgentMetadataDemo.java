

package org.apache.ignite.console.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.ignite.console.agent.db.DBInfo;
import org.apache.ignite.console.agent.db.DataSourceManager;
import org.h2.tools.RunScript;
import org.h2.tools.Server;
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
    private static final AtomicBoolean initLatch = new AtomicBoolean();

    /** */
    private static final String DEMO_SCRIPT_PATH = "demo/db-init.sql";

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
    public static Connection testDrive() throws SQLException {
        if (initLatch.compareAndSet(false, true)) {
            log.info("DEMO: Prepare in-memory H2 database...");

            try {
                Class.forName("org.h2.Driver");

                Connection conn = DriverManager.getConnection("jdbc:h2:mem:demo-db;DB_CLOSE_DELAY=-1", "sa", "");

                File sqlScript = resolvePath(DEMO_SCRIPT_PATH);

                if (sqlScript == null)
                    throw new FileNotFoundException(DEMO_SCRIPT_PATH);

                RunScript.execute(conn, new FileReader(sqlScript));

                log.info("DEMO: Sample tables created.");

                conn.close();

                Server.createTcpServer("-tcpDaemon").start();

                log.info("DEMO: TcpServer stared.");

                log.info("DEMO: JDBC URL for test drive metadata load: jdbc:h2:mem:demo-db");

            }
            catch (ClassNotFoundException e) {
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
       
        return DriverManager.getConnection("jdbc:h2:mem:demo-db;DB_CLOSE_DELAY=-1", "sa", "");
    }
}
