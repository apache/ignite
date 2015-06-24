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

import org.apache.ignite.*;
import org.apache.ignite.yardstick.*;
import org.apache.ignite.yardstick.cache.model.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Ignite benchmark that performs put and get operations.
 */
public class IgnitePutGetClientPerThreadBenchmark extends IgniteCacheAbstractBenchmark {
    /** Id sequence. */
    private AtomicInteger idSeq = new AtomicInteger(0);

    /** Nodes. */
    private Map<Integer, IgniteNode> nodes = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        IgniteCache<Integer, Object> cache = (IgniteCache<Integer, Object>)ctx.get(0);

        if (cache == null) {
            IgniteNode node = new IgniteNode(true);

            node.start(cfg);

            cache = node.ignite().cache("atomic");

            ctx.put(0, cache);

            nodes.put(idSeq.getAndIncrement(), node);
        }

        int key = nextRandom(args.range());

        Object val = cache.get(key);

        if (val != null)
            key = nextRandom(args.range());

        cache.put(key, new SampleValue(key));

        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        for (IgniteNode node : nodes.values())
            node.stop();

        super.tearDown();
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic");
    }
}
