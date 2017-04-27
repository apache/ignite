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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.yardstick.cache.model.SampleValue;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs get operations.
 */
abstract class IgniteBinaryIdentityBenchmark extends IgniteCacheAbstractBenchmark<BinaryObject, SampleValue> {
    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        println(cfg, "Populating data...");

        long start = System.nanoTime();

        try (IgniteDataStreamer<BinaryObject, Object> dataLdr = ignite().dataStreamer(cache.getName())) {
            for (int i = 0; i < args.range() && !Thread.currentThread().isInterrupted();) {
                dataLdr.addData(createKey(i), new SampleValue(i));

                if (++i % 100000 == 0)
                    println(cfg, "Items populated: " + i);
            }
        }

        println(cfg, "Finished populating data in " + ((System.nanoTime() - start) / 1_000_000) + " ms.");
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<BinaryObject, SampleValue> cache() {
        return ignite().cache("atomic").withKeepBinary();
    }

    /**
     * @param key Base key value.
     * @return Binary key.
     */
    abstract BinaryObject createKey(int key);

    /**
     * @param key Key field value.
     * @return Binary object without hash code explicitly set at build time.
     */
    BinaryObject createFieldsIdentityBinaryKey(int key) {
        BinaryObjectBuilder bldr = ignite().binary().builder("BinaryKeyWithFieldsIdentity");

        setBuilderFields(bldr, key);

        return bldr.build();
    }

    /**
     * @param builder Builder.
     * @param key Key field value.
     */
    private static void setBuilderFields(BinaryObjectBuilder builder, int key) {
        builder.setField("f1", 1);

        builder.setField("f2", "SomeString");

        builder.setField("f3", (long) key);
    }
}
