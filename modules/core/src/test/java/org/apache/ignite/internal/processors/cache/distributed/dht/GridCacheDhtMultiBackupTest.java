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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridCacheDhtMultiBackupTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridCacheDhtMultiBackupTest() {
        super(false /* don't start grid. */);
    }

    /**
     * @throws Exception If failed
     */
    public void testPut() throws Exception {
        try {
            Ignite g = G.start("examples/config/example-cache.xml");

            if (g.cluster().nodes().size() < 5)
                U.warn(log, "Topology is too small for this test. " +
                    "Run with 4 remote nodes or more having large number of backup nodes.");

            g.compute().run(new CAX() {
                    @IgniteInstanceResource
                    private Ignite g;

                    @Override public void applyx() {
                        X.println("Checking whether cache is empty.");

                        IgniteCache<SampleKey, SampleValue> cache = g.cache("partitioned");

                        assert cache.localSize() == 0;
                    }
                }
            );

            IgniteCache<SampleKey, SampleValue> cache = g.cache("partitioned");

            int cnt = 0;

            for (int key = 0; key < 1000; key++) {
                SampleKey key1 = new SampleKey(key);

                if (!g.cluster().localNode().id().equals(g.cluster().mapKeyToNode("partitioned", key1).id())) {
                    cache.put(key1, new SampleValue(key));

                    cnt++;
                }
            }

            X.println(">>> Put count: " + cnt);
        }
        finally {
            G.stopAll(false);
        }
    }

    /**
     *
     */
    private static class SampleKey implements Serializable {
        /** */
        private int key;

        /**
         * @param key
         */
        private SampleKey(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof SampleKey && ((SampleKey)obj).key == key;
        }
    }

    /**
     *
     */
    private static class SampleValue implements Serializable {
        /** */
        private int val;

        /**
         * @param val
         */
        private SampleValue(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof SampleValue && ((SampleValue)obj).val == val;
        }
    }
}