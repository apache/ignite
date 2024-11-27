package org.apache.ignite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.SessionContextProviderResource;
import org.apache.ignite.session.SessionContext;
import org.apache.ignite.session.SessionContextProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** */
public class JdbcSetClientInfoTest extends GridCommonAbstractTest {
    /** */
    private static final String SESSION_ID = "SESSION_ID";

    /** */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1?queryEngine=calcite";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setSqlConfiguration(new SqlConfiguration()
            .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration().setDefault(true)));

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setSqlSchema("PUBLIC")
            .setSqlFunctionClasses(SessionContextFunctions.class));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        IgniteEx ign = startGrids(3);

        ignQuery(ign, "create table PUBLIC.MYTABLE(id int primary key, sessionId varchar);");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testSetClientInfo() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            checkSessionId(conn, null);
            checkSessionId(conn, "1234");
        }
    }

    /** */
    @Test
    public void testResetClientInfo() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            checkSessionId(conn, "1234");
            checkSessionId(conn, "4567");
            checkSessionId(conn, null);
        }
    }

    /** */
    private void checkSessionId(Connection conn, @Nullable String sesId) throws Exception {
        setClientInfo(conn, sesId);

        ResultSet set = jdbcQuery(conn, "select sessionId() as SESSION_ID;");

        set.next();

        String actSesId = set.getString("SESSION_ID");

        assertEquals(sesId, actSesId);
    }

    /** */
    @Test
    public void testWhereClause() throws Exception {
        for (int i = 0; i < 100; i++) {
            String sesId = i % 2 == 0 ? "1" : "2";

            ignQuery(grid(0), "insert into PUBLIC.MYTABLE(id, sessionId) values (?, ?);", i, sesId);
        }

        try (Connection conn = DriverManager.getConnection(URL)) {
            for (String sesId: F.asList("1", "2")) {

                setClientInfo(conn, sesId);

                ResultSet set = jdbcQuery(conn, "select * from PUBLIC.MYTABLE where sessionId = sessionId();");

                int size = 0;

                while (set.next()) {
                    String actSesId = set.getString("sessionId");

                    assertEquals(sesId, actSesId);

                    size++;
                }

                assertEquals(50, size);
            }
        }
    }

    /** */
    @Test
    public void testInsertClause() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            for (int i = 0; i < 100; i++) {
                String sesId = i % 2 == 0 ? "1" : "2";

                setClientInfo(conn, sesId);

                jdbcQuery(conn, "insert into PUBLIC.MYTABLE(id, sessionId) values (" + i + ", sessionId());");
            }
        }

        List<List<?>> res = ignQuery(grid(0), "select * from PUBLIC.MYTABLE where sessionId = 1");

        assertEquals(50, res.size());

        res = ignQuery(grid(0), "select * from PUBLIC.MYTABLE where sessionId = 2");

        assertEquals(50, res.size());
    }

    /** */
    @Test
    public void testNestedQuery() throws Exception {
        for (int i = 0; i < 100; i++) {
            String sesId = i % 2 == 0 ? "1" : "2";

            ignQuery(grid(0), "insert into PUBLIC.MYTABLE(id, sessionId) values (?, ?);", i, sesId);
        }

        try (Connection conn = DriverManager.getConnection(URL)) {
            String sesId = "1";

            setClientInfo(conn, sesId);

            ResultSet set = jdbcQuery(conn, "select * from PUBLIC.MYTABLE where sessionId = (select sessionId());");

            int size = 0;

            while (set.next()) {
                String actSesId = set.getString("sessionId");

                assertEquals(sesId, actSesId);

                size++;
            }

            assertEquals(50, size);
        }
    }

    /** */
    @Test
    public void testMultipleQueries() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            setClientInfo(conn, "1");

            jdbcQuery(conn,
                "insert into PUBLIC.MYTABLE(id, sessionId) values (0, sessionId());" +
                    "insert into PUBLIC.MYTABLE(id, sessionId) values (1, sessionId());" +
                    "insert into PUBLIC.MYTABLE(id, sessionId) values (2, sessionId());" +
                    "insert into PUBLIC.MYTABLE(id, sessionId) values (3, sessionId());" +
                    "insert into PUBLIC.MYTABLE(id, sessionId) values (4, sessionId())");
        }

        List<List<?>> res = ignQuery(grid(0), "select * from PUBLIC.MYTABLE where sessionId = 1");

        assertEquals(5, res.size());
    }

    /** */
    @Test
    public void testUpdateStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            setClientInfo(conn, "1");

            Statement statement = conn.createStatement();

            statement.executeUpdate(
                "insert into PUBLIC.MYTABLE(id, sessionId) values (0, sessionId());" +
                "insert into PUBLIC.MYTABLE(id, sessionId) values (1, sessionId());" +
                "insert into PUBLIC.MYTABLE(id, sessionId) values (2, sessionId());" +
                "insert into PUBLIC.MYTABLE(id, sessionId) values (3, sessionId());" +
                "insert into PUBLIC.MYTABLE(id, sessionId) values (4, sessionId())");
        }

        List<List<?>> res = ignQuery(grid(0), "select * from PUBLIC.MYTABLE where sessionId = 1");

        assertEquals(5, res.size());
    }

    /** */
    @Test
    public void testBatchStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            setClientInfo(conn, "1");

            Statement statement = conn.createStatement();

            for (int i = 0; i < 5; i++)
                statement.addBatch("insert into PUBLIC.MYTABLE(id, sessionId) values (" + i + ", sessionId());");

            statement.executeBatch();
        }

        List<List<?>> res = ignQuery(grid(0), "select * from PUBLIC.MYTABLE where sessionId = 1");

        assertEquals(5, res.size());
    }

    /** */
    private void setClientInfo(Connection conn, @Nullable String sesId) throws Exception {
        Properties props = new Properties();

        if (sesId != null)
            props.put(SESSION_ID, sesId);

        conn.setClientInfo(props);
    }

    /** */
    private List<List<?>> ignQuery(IgniteEx ign, String sql, Object... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }

    /** */
    private @Nullable ResultSet jdbcQuery(Connection conn, String sql) throws SQLException {
        Statement statement = conn.createStatement();

        boolean res = statement.execute(sql);

        if (res)
            return statement.getResultSet();

        return null;
    }

    /** */
    public static class SessionContextFunctions {
        /** */
        @SessionContextProviderResource
        public SessionContextProvider sesCtxProv;

        /** */
        @QuerySqlFunction
        public String sessionId() {
            SessionContext sesCtx = sesCtxProv.getSessionContext();

            return sesCtx == null ? null : sesCtx.getAttribute(SESSION_ID);
        }
    }
}
