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
