package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Date;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class CreateTableWithDateKeySelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_COUNT = 1;

    /** */
    public void testPassTableWithDateKeyCreation() throws Exception {
        Ignite node = this.startGrid();
        IgniteCache<Object, Object> cache = node.getOrCreateCache(DEFAULT_CACHE_NAME);


        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "CREATE TABLE Tab(id DATE primary key, dateField DATE)"))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(0L, rows.get(0).get(0));
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_COUNT, false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** */
    public class Tab{
        /** */
        @QuerySqlField(index = true)
        private Date id;

        /** */
        @QuerySqlField(index = true)
        private Date dateField;

        /** */
        public Date getId() {
            return id;
        }

        /** */
        public void setId(Date id) {
            this.id = id;
        }

        /** */
        public Date getDateField() {
            return dateField;
        }

        /** */
        public void setDateField(Date dateField) {
            this.dateField = dateField;
        }
    }
}
