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

package org.apache.ignite.console.demo.service;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.console.demo.AgentDemoUtils;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;

/**
 * Demo service. Create cache and populate it by random int pairs.
 */
public class DemoRandomCacheLoadService implements Service {
    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Thread pool to execute cache load operations. */
    private ScheduledExecutorService cachePool;

    /** */
    public static final String RANDOM_CACHE_NAME = "RandomCache";

    /** Employees count. */
    private static final int RND_CNT = 1024;

    /** */
    private static final Random rnd = new Random();

    /** Maximum count read/write key. */
    private final int cnt;

    /**
     * @param cnt Maximum count read/write key.
     */
    public DemoRandomCacheLoadService(int cnt) {
        this.cnt = cnt;
    }

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        if (cachePool != null)
            cachePool.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) throws Exception {
        ignite.getOrCreateCache(cacheRandom());

        cachePool = AgentDemoUtils.newScheduledThreadPool(2, "demo-sql-random-load-cache-tasks");
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) throws Exception {
        cachePool.scheduleWithFixedDelay(new Runnable() {
            @Override public void run() {
                try {
                    for (String cacheName : ignite.cacheNames()) {
                        IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

                        if (cache != null &&
                            !DemoCachesLoadService.DEMO_CACHES.contains(cacheName) &&
                            !DFLT_SCHEMA.equalsIgnoreCase(cache.getConfiguration(CacheConfiguration.class).getSqlSchema())) {
                            for (int i = 0, n = 1; i < cnt; i++, n++) {
                                Integer key = rnd.nextInt(RND_CNT);
                                Integer val = rnd.nextInt(RND_CNT);

                                cache.put(key, val);

                                if (rnd.nextInt(100) < 30)
                                    cache.remove(key);
                            }
                        }
                    }
                }
                catch (Throwable e) {
                    if (!e.getMessage().contains("cache is stopped"))
                        ignite.log().error("Cache write task execution error", e);
                }
            }
        }, 10, 3, TimeUnit.SECONDS);
    }

    /**
     * Configure cacheCountry.
     */
    private static <K, V> CacheConfiguration<K, V> cacheRandom() {
        CacheConfiguration<K, V> ccfg = new CacheConfiguration<>(RANDOM_CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setQueryDetailMetricsSize(10);
        ccfg.setStatisticsEnabled(true);
        ccfg.setIndexedTypes(Integer.class, Integer.class);

        return ccfg;
    }
}
