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

package org.apache.ignite.yardstick.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.yardstick.cache.model.PersonTextIndex;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Base class for cache queries benchmarks.
 */
abstract class IgniteCacheQueryGetAllBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** */
    protected static final String namePrefix = "personName";

    /** Cache query keep binary flag. */
    private boolean keepBinary;

    /** Cache query pageSize. */
    private int pageSize;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        keepBinary = args.getBooleanParameter("keepBinary", false);
        pageSize = args.getIntParameter("pageSize", Query.DFLT_PAGE_SIZE);

        println("Parameters of test: [keepBinary=" + keepBinary + "; pageSize=" + pageSize + "].");

        loadCachesData();
    }

    /** {@inheritDoc} */
    @Override protected void loadCacheData(String cacheName) {
        try (IgniteDataStreamer<Integer, PersonTextIndex> dataLdr = ignite().dataStreamer(cacheName)) {
            for (int i = 0; i < args.range() && !Thread.currentThread().isInterrupted(); i++) {
                dataLdr.addData(i, new PersonTextIndex(i, namePrefix + i));

                if (i % 100000 == 0)
                    println(cfg, "Populated persons: " + i);
            }
        }
    }

    /** */
    protected boolean testCacheQuery(Query<?> cacheQry) {
        IgniteCache<Integer, Object> cache = cacheForOperation();

        if (keepBinary)
            cache = cache.withKeepBinary();

        cacheQry.setPageSize(pageSize);

        QueryCursor<?> cursor = cache.query(cacheQry);

        for (Object o: cursor) {
            // No-op.
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("query");
    }
}
