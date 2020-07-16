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

package org.apache.ignite.internal.ducktest;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 *
 */
public class SingleKeyTxStreamerApplication extends IgniteAwareApplication {
    /**
     * @param ignite Ignite.
     */
    public SingleKeyTxStreamerApplication(Ignite ignite) {
        super(ignite);
    }

    /** {@inheritDoc} */
    @Override public void run(String[] args) {
        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(args[0]);

        int warmup = Integer.parseInt(args[1]);

        long max = -1;

        int key = 10_000_000;

        int cnt = 0;

        long initTime = 0;

        boolean record = false;

        while (!terminated()) {
            cnt++;

            long start = System.currentTimeMillis();

            cache.put(key++, key);

            long finish = System.currentTimeMillis();

            long time = finish - start;

            if (!record && cnt > warmup) {
                record = true;

                initTime = System.currentTimeMillis();;

                markInitialized();
            }

            if (record) {
                if (max < time)
                    max = time;
            }

            if (cnt % 1000 == 0)
                log.info("Streamed " + cnt + " transactions [max=" + max + "]");
        }

        recordResult("WORST_LATENCY", max);
        recordResult("STREAMED", cnt - warmup);
        recordResult("MEASURE_DURATION", System.currentTimeMillis() - initTime);

        markFinished();
    }
}
