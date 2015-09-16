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

package org.apache.ignite.yardstick.cache.failover;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs get operations.
 */
public class IgniteFailoverBenchmark extends IgniteCacheAbstractBenchmark {
    /** */
    private final ConcurrentMap<Integer, BenchmarkConfiguration> srvsCfgs = new ConcurrentHashMap<>();

    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        println(">>>>>>>> is client mode = " + ignite().configuration().isClientMode());

        if (cfg.memberId() == 0) {
            Thread thread = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        // Read servers configs from cache and destroy it.
                        IgniteCache<Integer, BenchmarkConfiguration> srvsCfgsCache = ignite().
                            getOrCreateCache(new CacheConfiguration<Integer, BenchmarkConfiguration>().
                                setName("serversConfigs"));

                        for (Cache.Entry<Integer, BenchmarkConfiguration> e : srvsCfgsCache) {
                            println("Read entry from 'serversConfigs' cache =" + e);

                            srvsCfgs.put(e.getKey(), e.getValue());
                        }

                        srvsCfgsCache.destroy();

                        // TODO check srvsCfgs.size() == servers.size()

                        final int backupsCnt = 1; // TODO
                        assert backupsCnt >= 1 : "Backups=" + backupsCnt;

                        ThreadLocalRandom random = ThreadLocalRandom.current();

                        Thread.sleep(10_000); // TODO warmup

                        while (!Thread.currentThread().isInterrupted()) {
                            int numberNodesToRestart = random.nextInt(1, backupsCnt + 1);

                            List<Integer> ids = new ArrayList<>();

                            ids.addAll(srvsCfgs.keySet());

                            Collections.shuffle(ids, random);

                            println("Number nodes to restart = " + numberNodesToRestart + ", shuffled ids = " + ids);

                            for (int i = 0; i < numberNodesToRestart; i++) {
                                Integer id = ids.get(i);

                                BenchmarkConfiguration bc = srvsCfgs.get(id);

                                RestartUtils.Result result = RestartUtils.kill9(bc, true);

                                println(">>>>>>>>>RESULT<<<<<<<<<<\n" + result);
                            }

                            // TODO wait for all nodes stop

                            Thread.sleep(2_000); // TODO sleep

                            for (int i = 0; i < numberNodesToRestart; i++) {
                                Integer id = ids.get(i);

                                BenchmarkConfiguration bc = srvsCfgs.get(id);

                                RestartUtils.Result result = RestartUtils.start(bc, true);

                                println(">>>>>>>>>RESULT 2<<<<<<<<<<\n" + result);
                            }

                            Thread.sleep(5_000); //TODO delay
                        }
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, "restarter");

            thread.setDaemon(true);

            thread.start();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key = nextRandom(args.range());

        cache.get(key);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic");
    }
}
