package org.apache.ignite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import javax.cache.configuration.Factory;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class SelectUserAttributesTest extends GridCommonAbstractTest {
    /** */
    private static final String SESSION_ID_ATTR = "sessionId";

    /** */
    private static final String SESSION_ID_SRV = "1";

    /** */
    private static final String SESSION_ID_CLN = "2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setSqlConfiguration(new SqlConfiguration()
            .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration().setDefault(true)));

        cfg.setUserAttributes(F.asMap(SESSION_ID_ATTR, SESSION_ID_SRV));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testThinJdbc() throws Exception {
        startGrid(0);

        String url = "jdbc:ignite:thin://127.0.0.1?queryEngine=calcite";
        url += "&userAttributesFactory=" + UserAttributesFactory.class.getName();

        try (Connection conn = DriverManager.getConnection(url)) {
            Statement statement = conn.createStatement();

            boolean res = statement.execute("select USER_ATTRIBUTE('sessionId') as SESSION_ID;");

            assert res;

            ResultSet set = statement.getResultSet();
            res = set.next();

            assert res;

            String sesId = set.getString("SESSION_ID");

            assertEquals(SESSION_ID_CLN, sesId);
        }
    }

    /** */
    @Test
    public void testThinClient() throws Exception {
        startGrid(0);

        ClientConfiguration clnCfg = new ClientConfiguration();
        clnCfg.setAddresses("127.0.0.1:10800");
        clnCfg.setUserAttributes(new UserAttributesFactory().create());

        try (IgniteClient cln = Ignition.startClient(clnCfg)) {
            List<List<?>> res = cln.query(new SqlFieldsQuery("select USER_ATTRIBUTE('sessionId') as SESSION_ID;")).getAll();

            assertEquals(1, res.size());
            assertEquals(1, res.get(0).size());
            assertEquals(SESSION_ID_CLN, res.get(0).get(0));
        }
    }

    /** */
    @Test
    public void testThickClient() throws Exception {
        startGrid(0);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(1));
        cfg.setClientMode(true);
        cfg.setUserAttributes(new UserAttributesFactory().create());

        List<List<?>> res = startClientGrid(cfg)
            .createCache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQuery("select USER_ATTRIBUTE('sessionId') as SESSION_ID;")).getAll();

        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
        assertEquals(SESSION_ID_CLN, res.get(0).get(0));
    }

    /** */
    @Test
    public void testServer() throws Exception {
        List<List<?>> res = startGrid(0)
            .createCache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQuery("select USER_ATTRIBUTE('sessionId') as SESSION_ID;")).getAll();

        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
        assertEquals(SESSION_ID_SRV, res.get(0).get(0));
    }

    /** */
    @Test
    public void testSubQuery() throws Exception {
        Ignite ign = startGrids(3);

        IgniteCache<?, ?> cache = ign.createCache(new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setSqlSchema("PUBLIC"));

        cache.query(new SqlFieldsQuery("create table PUBLIC.MYTABLE(id int primary key, val varchar);")).getAll();

        for (int i = 0; i < 100; i++)
            cache.query(new SqlFieldsQuery("insert into PUBLIC.MYTABLE(id, val) values (?, ?);").setArgs(i, i));

        ClientConfiguration clnCfg = new ClientConfiguration();
        clnCfg.setAddresses("127.0.0.1:10800");
        clnCfg.setUserAttributes(F.asMap("limit", "50"));

        try (IgniteClient cln = Ignition.startClient(clnCfg)) {
            List<List<?>> res = cln.query(new SqlFieldsQuery("select * from PUBLIC.MYTABLE where val < cast(USER_ATTRIBUTE('limit') as int);")).getAll();

            assertEquals(50, res.size());
        }
    }

    /** */
    public static class UserAttributesFactory implements Factory<Map<String, String>> {
        /** */
        @Override public Map<String, String> create() {
            return F.asMap(SESSION_ID_ATTR, SESSION_ID_CLN);
        }
    }
}
