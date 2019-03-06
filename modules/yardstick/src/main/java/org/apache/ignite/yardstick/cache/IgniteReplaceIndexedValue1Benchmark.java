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

package org.apache.ignite.yardstick.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.yardstick.cache.model.Person1;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Ignite benchmark that performs SQL UPDATE operations.
 */
public class IgniteReplaceIndexedValue1Benchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** {@inheritDoc} */
    @Override public void setUp(final BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        final AtomicInteger i = new AtomicInteger();

        Collection<Thread> setupThreads = new ArrayList<>(cfg.threads());

        for (int j = 0; j < cfg.threads(); j++) {
            Thread t = new Thread() {
                /** {@inheritDoc} */
                @Override public void run() {
                    int k;

                    while ((k = i.getAndIncrement()) < args.range()) {
                        cache().put(k, new Person1(k));
                        if (++k % 100000 == 0)
                            BenchmarkUtils.println(cfg, "REPLACE setUp: have successfully put " + k + " items");
                    }
                }
            };

            setupThreads.add(t);

            t.start();
        }

        for (Thread t : setupThreads)
            t.join();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        IgniteCache<Integer, Object> cache = cacheForOperation();

        cache.replace(rnd.nextInt(args.range()), new Person1(rnd.nextInt(args.range())));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic-index");
    }
}

