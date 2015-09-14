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

import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Ignite benchmark that performs get operations.
 */
public class IgniteFailoverBenchmark extends IgniteCacheAbstractBenchmark {
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        if (cfg.memberId() == 0) {
            Thread thread = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        Thread.sleep(20_000);

                        while (!Thread.currentThread().isInterrupted()) {
                            RestartUtils.kill9("${REMOTE_USER}", "localhost");

                            RestartUtils.start();

                            Thread.sleep(10_000);
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
        if (cfg.memberId() == 0)
            return true;

        ignite().log().info(">>>>>>>> client mode = " + ignite().configuration().isClientMode());

        int key = nextRandom(args.range());

        cache.get(key);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic");
    }
}
