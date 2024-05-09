package org.apache.ignite.cache.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class IndexQueryPaginationTest extends GridCommonAbstractTest {
    /** */
    private static final int PAGE_SIZE = 512;

    /** */
    private Map<Integer, Integer> entriesAndReqs;

    /** */
    private Ignite grid;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        entriesAndReqs = new HashMap<Integer, Integer>() {{
            put(100, 0);
            put(1000, 0);
            put(5000, 4);
            put(10_000, 9);
            put(50_000, 49);
            put(100_000, 99);
        }};

        grid = startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>("cache")
            .setIndexedTypes(Integer.class, Person.class)
            .setCacheMode(CacheMode.PARTITIONED);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Insert data into a cache.
     *
     * @param ignite Ignite instance.
     * @param cache Cache.
     * @param entries Number of entries.
     */
    private void insertData(Ignite ignite, IgniteCache<Integer, Person> cache, int entries) {
        try (IgniteDataStreamer<Integer, Person> streamer = ignite.dataStreamer(cache.getName())) {
            for (int i = 0; i < entries; i++)
                streamer.addData(i, new Person(i));
        }
    }

    /**
     * Check if the number of next page requests is correct while executing an index query.
     */
    @Test
    public void nextPageRequestsTest() {
        final IgniteCache<Integer, Person> cache = grid.cache("cache");

        for (Map.Entry<Integer, Integer> e : entriesAndReqs.entrySet()) {
            int entries = e.getKey();
            int reqsExpected = e.getValue();

            insertData(grid, cache, entries);

            TestRecordingCommunicationSpi.spi(grid).record(GridCacheQueryRequest.class);

            QueryCursor<Cache.Entry<Integer, Person>> cursor = cache.query(
                new IndexQuery<Integer, Person>(Person.class)
                    .setPageSize(PAGE_SIZE));

            assertEquals(entries, cursor.getAll().size());

            List<Object> reqs = TestRecordingCommunicationSpi.spi(grid).recordedMessages(true)
                .stream()
                .map(o -> (GridCacheQueryRequest)o)
                .filter(o -> o.type() == null)
                .collect(Collectors.toList());

            assertEquals(reqsExpected, reqs.size());

            cache.clear();
        }
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(index = true) final int id;

        /** */
        Person(int id) {
            this.id = id;
        }
    }
}
