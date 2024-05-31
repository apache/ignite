/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.query;

import java.util.ArrayList;
import java.util.List;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.query.ThinClientIndexQueryTest.getFilteredMessages;

/** */
@RunWith(Parameterized.class)
public class IndexQueryPaginationTest extends GridCommonAbstractTest {
    /** */
    private static final int PAGE_SIZE = 512;

    /** */
    private static final int NODES = 2;

    /** */
    private Ignite grid;

    /** */
    private IgniteCache<Integer, Person> cache;

    /** */
    @Parameterized.Parameter
    public int entries;

    /** */
    @Parameterized.Parameters(name = "entries={0}")
    public static Object[] params() {
        return new Object[] {100, 1000, 5000, 10_000, 50_000, 100_000};
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        grid = startGrids(NODES);

        cache = grid.cache("cache");

        insertData(grid, cache, entries);
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

        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(ccfg);
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
     * Check if the number and the size of next page requests and responses are correct while executing an index query.
     */
    @Test
    public void nextPageTest() {
        int locNodeEntries = cache.query(new ScanQuery<Integer, Person>().setLocal(true)).getAll().size();
        int remNodeEntries = entries - locNodeEntries;

        int reqsExpected = (remNodeEntries + PAGE_SIZE - 1) / PAGE_SIZE;

        int remNodeLastPageEntries = remNodeEntries % PAGE_SIZE;

        for (int i = 0; i < NODES; i++)
            TestRecordingCommunicationSpi.spi(grid(i)).record(GridCacheQueryRequest.class, GridCacheQueryResponse.class);

        QueryCursor<Cache.Entry<Integer, Person>> cursor = cache.query(
            new IndexQuery<Integer, Person>(Person.class).setPageSize(PAGE_SIZE));

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

            if (i == reqsSize - 1 && remNodeLastPageEntries != 0)
                assert respData == remNodeLastPageEntries;
            else
                assert respData == reqPage;
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
