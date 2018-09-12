package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class CreateTableWithDateKeySelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_COUNT = 1;

    /** */
    private static Ignite ignite;

    /** */
    public void testPassTableWithDateKeyCreation(){
        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME+"1");

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "CREATE TABLE Tab1(id DATE primary key, dateField DATE)"))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(0L, rows.get(0).get(0));
        }
    }

    /** */
    public void testPassTableWithTimeKeyCreation(){
        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME+"2");

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "CREATE TABLE Tab2(id TIME primary key, dateField TIME)"))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(0L, rows.get(0).get(0));
        }
    }

    /** */
    public void testPassTableWithTimeStampKeyCreation(){
        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME+"3");

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "CREATE TABLE Tab3(id TIMESTAMP primary key, dateField TIMESTAMP)"))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(0L, rows.get(0).get(0));
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGridsMultiThreaded(NODES_COUNT, false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }
}
