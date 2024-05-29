package org.apache.ignite.cache.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryResponse;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.query.ThinClientIndexQueryTest.getFilteredMessages;

/** */
public class IndexQueryPaginationTest extends GridCommonAbstractTest {
    /** */
    private static final int PAGE_SIZE = 512;

    /** */
    private static final int NODES = 2;

    /** */
    private Map<Integer, Integer> entriesAndReqs;

    /** */
    private Ignite grid;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        entriesAndReqs = new HashMap<Integer, Integer>() {{
            put(100, 1);
            put(1000, 1);
            put(5000, 5);
            put(10_000, 10);
            put(50_000, 50);
            put(100_000, 100);
        }};

        grid = startGrids(NODES);
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

            int localNodeEntries = cache.query(new ScanQuery<Integer, Person>().setLocal(true)).getAll().size();
            int remoteNodeEntries = entries - localNodeEntries;

            int remoteNodelastPageEntries = remoteNodeEntries % PAGE_SIZE;

            for (int i = 0; i < NODES; i++) {
                TestRecordingCommunicationSpi.spi(grid(i)).record(
                    GridCacheQueryRequest.class,
                    GridCacheQueryResponse.class);
            }

            QueryCursor<Cache.Entry<Integer, Person>> cursor = cache.query(
                new IndexQuery<Integer, Person>(Person.class)
                    .setPageSize(PAGE_SIZE));

            assert entries == cursor.getAll().size();

            List<Object> msgs = new ArrayList<>();

            for (int i = 0; i < NODES; i++)
                msgs.addAll(TestRecordingCommunicationSpi.spi(grid(i)).recordedMessages(true));

            List<GridCacheQueryRequest> reqs = getFilteredMessages(msgs, GridCacheQueryRequest.class);
            List<GridCacheQueryResponse> resp = getFilteredMessages(msgs, GridCacheQueryResponse.class);

            int reqsSize = reqs.size();

            assert reqsSize == reqsExpected && reqsSize == resp.size();

            for (int i = 0; i < reqsSize; i++) {
                int reqPage = reqs.get(i).pageSize();
                int respData = resp.get(i).data().size();

                assert reqPage == PAGE_SIZE;

                if (i == reqsSize - 1 && remoteNodelastPageEntries != 0)
                    assert respData == remoteNodelastPageEntries;
                else
                    assert respData == reqPage;
            }

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
