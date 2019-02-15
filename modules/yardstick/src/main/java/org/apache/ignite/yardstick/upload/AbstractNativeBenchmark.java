/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.yardstick.upload;

import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.upload.model.Values10;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Base class for benchmarks that perform upload using java api.
 */
public abstract class AbstractNativeBenchmark extends IgniteAbstractBenchmark {
    /** Number of entries to be uploaded during warmup. */
    private long insertRowsCnt;

    /** Name of the {@link #cache} */
    private String cacheName;

    /** Cache method {@link test(Map)} uploads data to */
    private IgniteCache<Long, Values10> cache;

    /**
     * Sets up benchmark: performs warmup on one cache and creates another for {@link #test(Map)} method.
     *
     * @param cfg Benchmark configuration.
     * @throws Exception - on error.
     */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        cacheName = this.getClass().getSimpleName();

        insertRowsCnt = args.upload.uploadRowsCnt();

        // Number of entries to be uploaded during test().
        long warmupRowsCnt = args.upload.warmupRowsCnt();

        // warmup
        BenchmarkUtils.println(cfg, "Starting custom warmup.");
        String warmupCacheName = cacheName + "Warmup";

        try (IgniteCache<Long, Values10> warmupCache = createCache(warmupCacheName)) {
            upload(warmupCacheName, warmupRowsCnt);
        }
        finally {
            ignite().destroyCache(warmupCacheName);
        }

        BenchmarkUtils.println(cfg, "Custom warmup finished.");

        // cache for benchmarked action
        cache = createCache(cacheName);
    }

    private IgniteCache<Long, Values10> createCache(String name) {
        CacheConfiguration<Long, Values10> cfg = new CacheConfiguration<>(name);

        if (args.atomicMode() != null)
            cfg.setAtomicityMode(args.atomicMode());

        return ignite().createCache(cfg);
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        try {
            if (cache == null)
                throw new IllegalStateException("Cache is null, probably an error during setUp or warmup");

            long size = cache.sizeLong();

            if (size != insertRowsCnt) {
                String msg = "Incorrect cache size: [actual=" + size + ", expected=" + insertRowsCnt +"].";

                BenchmarkUtils.println(cfg, "TearDown: " + msg);

                throw new RuntimeException(msg);
            }

            cache.close();

            ignite().destroyCache(cacheName);

        }
        catch (IgniteException ex) {
            BenchmarkUtils.println(cfg, "Could not close or destroy cache: " + ex);

            throw ex;
        }
        finally {
            super.tearDown();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        upload(cacheName, insertRowsCnt);

        return true;
    }

    /** Uploads {@param insertsCnt} to cache with name {@param cacheName} using java api. */
    protected abstract void upload(String cacheName, long insertsCnt);
}
