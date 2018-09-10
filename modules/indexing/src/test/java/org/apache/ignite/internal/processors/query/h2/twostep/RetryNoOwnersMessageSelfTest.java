package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.List;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_RETRY_TIMEOUT;

/** */
public class RetryNoOwnersMessageSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_COUNT = 4;

    /** */
    private static final String ORG = "org";

    /** */
    private static final String UPDATE_JOIN_SQL = "UPDATE Person set name = CONCAT('A ', name) ";

    /** */
    private IgniteCache<String, JoinSqlTestHelper.Person> personCache;

    /** */
    public void testRetryNoOwnersMessage() {
        SqlQuery<String, JoinSqlTestHelper.Person> qry = new SqlQuery<String, JoinSqlTestHelper.Person>(
            JoinSqlTestHelper.Person.class, JoinSqlTestHelper.JOIN_SQL).setArgs("Organization #0");

        qry.setDistributedJoins(true);

        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                String shutName = getTestIgniteInstanceName(NODES_COUNT-1);
                stopGrid(shutName, true, false);
                log.info(shutName+" stop grid signal is sent");
                return null;
            }
        });
        try {
            Thread.sleep(500);
        }
        catch (InterruptedException e) {
            // no op
        }
        try {
            List<Cache.Entry<String,JoinSqlTestHelper.Person>> prsns = personCache.query(qry).getAll();

            fail("No CacheException emitted. Collection size="+prsns.size());
        }
        catch (CacheException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Failed to calculate nodes for SQL query" +
                " (partition has no owners, but corresponding cache group has data nodes) ["));

            assertTrue(e.getMessage(), e.getMessage().contains("Failed to map SQL query to topology on data node"));
        }
    }

    /** */
    public void testRetryUpdateNoOwnersMessage() {
        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                String shutName = getTestIgniteInstanceName(NODES_COUNT-1);
                stopGrid(shutName, true, false);
                log.info(shutName+" stop grid signal is sent");
                return null;
            }
        });
        try {
            Thread.sleep(500);
        }
        catch (InterruptedException e) {
            // no op
        }
        try {
            personCache.query(new SqlFieldsQueryEx(UPDATE_JOIN_SQL,false).setSkipReducerOnUpdate(true));

            fail("No CacheException emitted.");
        }
        catch (CacheException e) {
            log.error(e.getMessage(), e);

            assertTrue(e.getMessage(), e.getMessage().contains("Failed to calculate nodes for SQL query" +
                " (partition has no owners, but corresponding cache group has data nodes) ["));

            assertTrue(e.getMessage(), e.getMessage().contains("Failed to determine nodes participating in the update."));
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(IGNITE_SQL_RETRY_TIMEOUT, "10");

        startGridsMultiThreaded(NODES_COUNT, false);

        personCache = ignite(0).getOrCreateCache(
            new CacheConfiguration<String, JoinSqlTestHelper.Person>("pers")
                .setBackups(0)
                .setIndexedTypes(String.class, JoinSqlTestHelper.Person.class)
                .setRebalanceThrottle(500)
        );

        final IgniteCache<String, JoinSqlTestHelper.Organization> orgCache = ignite(0).getOrCreateCache(
            new CacheConfiguration<String, JoinSqlTestHelper.Organization>(ORG)
                .setBackups(1)
                .setIndexedTypes(String.class, JoinSqlTestHelper.Organization.class)
                .setRebalanceThrottle(2000)
        );

        awaitPartitionMapExchange();

        JoinSqlTestHelper.populateDataIntoOrg(orgCache);

        JoinSqlTestHelper.populateDataIntoPerson(personCache);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }
}
