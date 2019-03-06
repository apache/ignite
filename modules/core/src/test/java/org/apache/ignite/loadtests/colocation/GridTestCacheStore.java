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

package org.apache.ignite.loadtests.colocation;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * Accenture cache store.
 */
public class GridTestCacheStore extends CacheStoreAdapter<GridTestKey, Long> {
    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** */
    @LoggerResource
    private IgniteLogger log;

    /**
     * Preload data from store. In this case we just auto-generate random values.
     *
     * @param clo Callback for every key.
     * @param args Optional arguments.
     */
    @Override public void loadCache(final IgniteBiInClosure<GridTestKey, Long> clo, Object... args) {
        // Number of threads is passed in as argument by caller.
        final int numThreads = (Integer)args[0];
        int entryCnt = (Integer)args[1];

        log.info("Number of load threads: " + numThreads);
        log.info("Number of cache entries to load: " + entryCnt);

        ExecutorService execSvc = Executors.newFixedThreadPool(numThreads);

        try {
            ExecutorCompletionService<Object> completeSvc = new ExecutorCompletionService<>(execSvc);

            final IgniteCache<GridTestKey, Long> cache = ignite.cache("partitioned");

            assert cache != null;

            final LongAdder adder = new LongAdder();

            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;

                final int perThreadKeys = entryCnt / numThreads;

                final int mod = entryCnt % numThreads;

                completeSvc.submit(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        int start = threadId * perThreadKeys;
                        int end = start + perThreadKeys;

                        if (threadId + 1 == numThreads)
                            end += mod;

                        for (long i = start; i < end; i++) {
                            if (ignite.affinity(cache.getName()).mapKeyToNode(GridTestKey.affinityKey(i)).isLocal()) { // Only add if key is local.
                                clo.apply(new GridTestKey(i), i);

                                adder.increment();
                            }

                            if (i % 10000 == 0)
                                log.info("Loaded " + adder.intValue() + " keys.");
                        }

                        return null;
                    }
                });
            }

            // Wait for threads to complete.
            for (int i = 0; i < numThreads; i++) {
                try {
                    completeSvc.take().get();
                }
                catch (InterruptedException | ExecutionException e) {
                    throw new CacheLoaderException(e);
                }
            }

            // Final print out.
            log.info("Loaded " + adder.intValue() + " keys.");
        }
        finally {
            execSvc.shutdown();
        }
    }

    /** {@inheritDoc} */
    @Override public Long load(GridTestKey key) {
        return null; // No-op.
    }

    /** {@inheritDoc} */
    @Override public void write(javax.cache.Cache.Entry<? extends GridTestKey, ? extends Long> e) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) {
        // No-op.
    }
}