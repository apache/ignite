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

package org.apache.ignite.internal.benchmarks.jmh.cache;

import java.util.concurrent.ThreadLocalRandom;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/** Puts to a cache with a continuous query of the calling node: with a local listener, or a remote filter. */
@SuppressWarnings("unchecked")
public class JmhContinuousQueryAllocationBenchmark extends JmhCacheAllocationAbstractBenchmark {
    /** Keys the setup writes. */
    private Integer[] keys;

    /** Values. */
    private IntValue[] vals;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        keys = new Integer[CNT];
        vals = new IntValue[CNT];

        for (int i = 0; i < CNT; i++) {
            keys[i] = i;
            vals[i] = new IntValue(i);
        }

        populate(keys, vals);
    }

    /**
     * Replaces the value of a key with the same value; the servers send each update to the query's listener.
     *
     * @param qry Continuous query with a local listener.
     */
    @Benchmark
    public void putWithListener(ListenerQuery qry) {
        put();
    }

    /**
     * Replaces the value of a key with the same value; the servers' filter drops every update.
     *
     * @param qry Continuous query with a remote filter.
     */
    @Benchmark
    public void putWithFilter(FilterQuery qry) {
        put();
    }

    /** Replaces the value of a random key with the same value. */
    private void put() {
        int i = ThreadLocalRandom.current().nextInt(CNT);

        cache.put(keys[i], vals[i]);
    }

    /** Continuous query whose local listener accepts every update, started before the benchmark method runs. */
    @State(Scope.Benchmark)
    public static class ListenerQuery {
        /** @param b Benchmark. */
        @Setup
        public void setup(JmhContinuousQueryAllocationBenchmark b) {
            b.cache.query(new ContinuousQuery<>().setLocalListener(evts -> evts.forEach(e -> { })));
        }
    }

    /** Continuous query whose remote filter drops every update, started before the benchmark method runs. */
    @State(Scope.Benchmark)
    public static class FilterQuery {
        /** @param b Benchmark. */
        @Setup
        public void setup(JmhContinuousQueryAllocationBenchmark b) {
            b.cache.query(new ContinuousQuery<>()
                .setLocalListener(evts -> evts.forEach(e -> { }))
                .setRemoteFilterFactory(FactoryBuilder.factoryOf(new DropAll())));
        }
    }

    /** Filter that drops every update. */
    private static class DropAll implements CacheEntryEventSerializableFilter<Object, Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<?, ?> evt) {
            return false;
        }
    }
}
