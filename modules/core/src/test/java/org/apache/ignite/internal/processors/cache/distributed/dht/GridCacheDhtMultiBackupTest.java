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
import org.junit.Test;

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
    @Test
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

                if (!g.cluster().localNode().id().equals(g.affinity("partitioned").mapKeyToNode(key1).id())) {
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
