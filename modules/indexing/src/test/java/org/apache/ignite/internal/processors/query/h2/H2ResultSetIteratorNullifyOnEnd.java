package org.apache.ignite.internal.processors.query.h2;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for iterator data link erasure after closing or completing
 */
public class H2ResultSetIteratorNullifyOnEnd extends GridCommonAbstractTest {
    /** */
    private static final int NODES_COUNT = 2;
    /** */
    private static final int PERSON_COUNT = 20;
    /** */
    private static final String SELECT_ALL_SQL = "select * from Person";
    /** */
    private IgniteCache<String, Person> personCache;
    /** */
    private IgniteH2Indexing h2Idx;

    /**
     * on close should remove links to data
     */
    public void testOnClose() {
        try {
            GridCloseableIterator it = h2Idx.queryLocalSql(
                h2Idx.schema(personCache.getName()),
                personCache.getName(),
                SELECT_ALL_SQL,
                null,
                Collections.emptySet(),
                "Person",
                null,
                null);
            if(H2ResultSetIterator.class.isAssignableFrom(it.getClass())){
                H2ResultSetIterator h2it = (H2ResultSetIterator)it;
                try {
                    h2it.onClose();
                }
                catch (IgniteCheckedException e) {
                    fail(e.getMessage());
                }
                assertNull(GridTestUtils.getFieldValue(h2it, H2ResultSetIterator.class, "data"));
            }else
                fail();
        }
        catch (IgniteCheckedException e) {
            fail(e.getMessage());
        }
    }
    /**
     * complete iterate should remove links to data
     */
    public void testOnComplete() {
         try {
            GridCloseableIterator it = h2Idx.queryLocalSql(
                h2Idx.schema(personCache.getName()),
                personCache.getName(),
                SELECT_ALL_SQL,
                null,
                Collections.emptySet(),
                "Person",
                null,
                null);
            if(H2ResultSetIterator.class.isAssignableFrom(it.getClass())){
                H2ResultSetIterator h2it = (H2ResultSetIterator)it;
                while( h2it.onHasNext())
                    h2it.onNext();
                assertNull(GridTestUtils.getFieldValue(h2it, H2ResultSetIterator.class, "data"));
            }else
                fail();
        }
        catch (IgniteCheckedException e) {
            fail(e.getMessage());
        }
    }
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        final Ignite ignite = startGridsMultiThreaded(NODES_COUNT, false);
        final GridQueryProcessor qryProc = grid(ignite.name()).context().query();
        h2Idx = GridTestUtils.getFieldValue(qryProc, GridQueryProcessor.class, "idx");

        personCache = ignite(0).getOrCreateCache(new CacheConfiguration<String, Person>("pers")
            .setIndexedTypes(String.class, Person.class));

        awaitPartitionMapExchange();

        populateDataIntoPerson(personCache);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Populate person cache with test data
     * @param cache @{IgniteCache}
     */
    private void populateDataIntoPerson(IgniteCache<String, Person> cache) {
        int personId = 0;
        for (int j = 0; j < PERSON_COUNT; j++) {
            Person prsn = new Person();
            prsn.setId("pers" + personId);
            prsn.setName("Person name #" + personId);
            cache.put(prsn.getId(), prsn);
            personId++;
        }
    }

    /**
     *
     */
    private static class Person {
        /** */
        @QuerySqlField(index = true)
        private String id;

        /** */
        @QuerySqlField(index = true)
        private String name;

        /** */
        public String getId() {

            return id;
        }

        /** */
        public void setId(String id) {

            this.id = id;
        }

        /** */
        public String getName() {

            return name;
        }

        /** */
        public void setName(String name) {

            this.name = name;
        }
    }
}
