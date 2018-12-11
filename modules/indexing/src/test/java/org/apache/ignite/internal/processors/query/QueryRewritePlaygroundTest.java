package org.apache.ignite.internal.processors.query;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class QueryRewritePlaygroundTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * @throws Exception If failed.
     */
    public void testVarious() throws Exception {
        executeSql("CREATE TABLE dept (id BIGINT PRIMARY KEY, name VARCHAR)");
        executeSql("CREATE TABLE emp (id BIGINT PRIMARY KEY, name VARCHAR, dept_id BIGINT, salary BIGINT)");

        //executeSql("SELECT emp.name, dept.name FROM emp, dept WHERE emp.dept_id=dept.id");
        executeSql("SELECT emp.name, dept.name FROM emp INNER JOIN dept ON emp.dept_id=dept.id");

//        executeSql("" +
//            "SELECT emp.name, (SELECT dept.name FROM dept WHERE emp.dept_id=dept.id)\n" +
//            "FROM emp\n" +
//            "WHERE emp.salary > 1000"
//        );
    }

    public static void executeSql(String sql) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }
        }
    }
}
