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

package org.apache.ignite.yardstick.datastreamer;

import java.util.Map;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/**
 *
 */
public class IgniteStreamerBenchmark extends IgniteAbstractBenchmark {
    /** Actual benchmark. */
    private IgniteAbstractStreamerBenchmark benchmark;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        benchmark = new IgniteAbstractStreamerBenchmark() {
            @Override <K, V> DataStreamer<K, V> dataStreamer(String cacheName) {
                return new DataStreamer<K, V>() {
                    private final IgniteDataStreamer<K, V> delegate;

                    {
                        delegate = ignite().dataStreamer(cacheName);

                        delegate.perNodeBufferSize(args.streamer.bufferSize());
                        delegate.allowOverwrite(args.streamer.allowOverwrite());
                    }

                    @Override public void addData(K key, V val) {
                        delegate.addData(key, val);
                    }

                    @Override public void addData(Map<K, V> entries) {
                        delegate.addData(entries);
                    }

                    @Override public void flush() {
                        delegate.flush();
                    }

                    @Override public void close() throws Exception {
                        delegate.close();
                    }
                };
            }

            @Override int cacheSize(String cacheName) {
                return ignite().cache(cacheName).size();
            }
        };

        benchmark.setUp(getClass().getSimpleName(), args, cfg, ignite().cacheNames());
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        benchmark.test();

        return false;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        benchmark.tearDown();

        super.tearDown();
    }
}
