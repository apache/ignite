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

package org.apache.ignite.yardstick.upload;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Benchmark that performs single upload of number of entries using {@link IgniteDataStreamer}.
 */
public class NativeStreamerBenchmark extends IgniteAbstractBenchmark {
    /** Number of entries to be uploaded during warmup. */
    private long insertRowsCnt;

    /** Number of entries to be uploaded during{@link #test(Map)}. */
    private static final long WARMUP_ROWS_CNT = 3_000_000;

    /** Name of the {@link #cache} */
    private static final String CACHE_NAME = NativeStreamerBenchmark.class.getSimpleName();

    /** Cache method {@link test(Map)} uploads data to */
    private IgniteCache<Long, Values10> cache;


    /**
     * Sets up benchmark: performs warmup on one cache and creates another for {@link #test(Map)} method.
     * @param cfg Benchmark configuration.
     * @throws Exception - on error.
     */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        insertRowsCnt = args.range();

        // warmup
        BenchmarkUtils.println(cfg, "Starting custom warmup.");
        String warmupCacheName = CACHE_NAME + "Warmup";

        try(IgniteCache<Long, Values10> warmupCache = ignite().createCache(warmupCacheName)) {
            upload(warmupCacheName, WARMUP_ROWS_CNT);
        }
        finally {
            ignite().destroyCache(warmupCacheName);
        }

        BenchmarkUtils.println(cfg, "Custom warmup finished.");

        // cache for benchmarked action
        cache = ignite().createCache(CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        try {
            if (cache != null) {
                cache.close();

                ignite().destroyCache(CACHE_NAME);
            }
        }
        catch (RuntimeException ex) {
            BenchmarkUtils.println(cfg, "Could not close and destroy cache: " + ex);
            throw ex;
        }
        finally {
            super.tearDown();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        upload(CACHE_NAME, insertRowsCnt);

        return true;
    }

    /**
     * Uploads randomly generated entries to specified cache
     * @param cacheName - name of the cache
     * @param insertsCnt - how many entries should be uploaded
     */
    private void upload(String cacheName,  long insertsCnt) {
        try (IgniteDataStreamer<Long, Values10> streamer = ignite().dataStreamer(cacheName)) {
            if (args.upload.streamerBufSize() !=null)
                streamer.perNodeBufferSize(args.upload.streamerBufSize());

            if (args.upload.streamerNodeParOps() != null)
                streamer.perNodeParallelOperations(args.upload.streamerNodeParOps());

            for (long i = 1; i <= insertsCnt; i++)
                //TODO: add batching support
                streamer.addData(i, new Values10());
        }
    }

}

/**
 * Describes data model.
 * Matches data model, defined in {@link QueryFactory#createTable()}
 */
class Values10 {
    final String val1;

    final long val2;

    final String val3;

    final long val4;

    final String val5;

    final long val6;

    final String val7;

    final long val8;

    final String val9;

    final long val10;

    /** Creates new object with randomly initialized fields */
    Values10(){
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        val1 = String.valueOf(rnd.nextLong());
        val2 = rnd.nextLong();

        val3 = String.valueOf(rnd.nextLong());
        val4 = rnd.nextLong();

        val5 = String.valueOf(rnd.nextLong());
        val6 = rnd.nextLong();

        val7 = String.valueOf(rnd.nextLong());
        val8 = rnd.nextLong();

        val9 = String.valueOf(rnd.nextLong());
        val10 = rnd.nextLong();
    }
}
