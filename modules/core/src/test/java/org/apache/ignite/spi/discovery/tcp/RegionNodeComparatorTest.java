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

package org.apache.ignite.spi.discovery.tcp;

import com.google.common.base.Supplier;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.tcp.internal.RegionNodeComparator;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests for class RegionNodeComparator
 */
public class RegionNodeComparatorTest extends GridCommonAbstractTest {
    /**
     * Test for basic mathematical properties comparison
     *
     * @throws Exception If failed.
     */
    public void testBasicProperties() throws Exception {
        final ComparatorTester<TcpDiscoveryNode> tester = new ComparatorTester<>();
        final Comparator<TcpDiscoveryNode> comparator = new RegionNodeComparator();

        final Supplier<TcpDiscoveryNode> factory = new NodeFactory();

        ComparatorTester.Pair<TcpDiscoveryNode> pair = tester.testEquals(factory, comparator);
        if (pair != null)
            fail("RegionNodeComparator don't see equals in two same node " + pairToString(pair));
        pair = tester.testSymmetry(factory, comparator);
        if (pair != null)
            fail("RegionNodeComparator don't work symmetry in two node " + pairToString(pair));
    }

    /**
     * @param pair Pair of nodes.
     * @return Debug string.
     */
    private static String pairToString(ComparatorTester.Pair<TcpDiscoveryNode> pair) {
        return "[firstNodeInternalOrder=" + pair.first.internalOrder() +
            ", firstNodeRegionId=" + pair.first.getAttributes().get("CLUSTER_REGION_ID") +
            ", secondNodeInternalOrder=" + pair.second.internalOrder() +
            ", secondNodeRegionId=" + pair.second.getAttributes().get("CLUSTER_REGION_ID") + ']';
    }

    /**
     * Test for correct sorting
     *
     * @throws Exception
     */
    public void testLogic() throws Exception {
        final int REGIONS = 13;
        final int N = REGIONS * 100;
        ArrayList<TcpDiscoveryNode> nodes = new ArrayList<>(N);
        final Comparator<TcpDiscoveryNode> comparator = new RegionNodeComparator();

        final NodeFactory factory = new NodeFactory();

        for (int i = 0; i < N; i++)
            nodes.add(factory.get(i % REGIONS));

        ArrayList<TcpDiscoveryNode> sortedNodes = new ArrayList<>(nodes);
        sortedNodes.sort(comparator);

        int i = 0;
        for (int r = 1; r <= REGIONS; r++) {
            for (int j = 0; j < N / REGIONS; j++) {
                assertEquals(r == REGIONS ? 0 : r, sortedNodes.get(i).internalOrder() % REGIONS);
                i++;
            }
        }
    }

    /**
     * Build nodes with correct ids and CLUSTER_REGION_IDs.
     */
    private static class NodeFactory implements Supplier<TcpDiscoveryNode> {
        /**  */
        final AtomicLong id = new AtomicLong(1L);
        /** */
        final Random random = new Random();

        /**
         * @return Node without id and CLUSTER_REGION_ID.
         */
        private TcpDiscoveryNode getRaw() {
            TcpDiscoveryNode node = new TcpDiscoveryNode(new UUID(0L, id.getAndIncrement()), Collections.singletonList("1.1.1.1"),
                Collections.singletonList("1.1.1.1"), 88, new DiscoveryMetricsProvider() {
                @Override
                public ClusterMetrics metrics() {
                    return null;
                }

                @Override
                public Map<Integer, CacheMetrics> cacheMetrics() {
                    return Collections.EMPTY_MAP;
                }
            }, new IgniteProductVersion(), null);

            return node;
        }

        /**
         * @return Node with sequential id and random CLUSTER_REGION_ID.
         */
        @Override
        public TcpDiscoveryNode get() {
            TcpDiscoveryNode node = getRaw();

            node.setAttributes(Collections.singletonMap("CLUSTER_REGION_ID", random.nextInt(10)));
            node.internalOrder(node.id().getLeastSignificantBits());
            return node;
        }

        /**
         * @param region CLUSTER_REGION_ID.
         * @return Node with sequential id and specific CLUSTER_REGION_ID.
         */
        public TcpDiscoveryNode get(long region) {
            TcpDiscoveryNode node = getRaw();

            node.setAttributes(Collections.singletonMap("CLUSTER_REGION_ID", region));
            node.internalOrder(node.id().getLeastSignificantBits());
            return node;
        }

    }

    /**
     * Class for testing comparators. In the future we may add new comparator, and then may use this class.
     */
    private static class ComparatorTester<T> {
        /**
         * The number of experiments
         */
        final int N;

        /**
         *
         */
        public ComparatorTester() {
            N = 1000;
        }

        /**
         * Test condition x == x
         *
         * @param factory
         * @param comparator
         * @return Return null if okay, or pair of values which make error if any.
         */
        Pair<T> testEquals(Supplier<T> factory, Comparator<T> comparator) {
            HashMap<T, T> set = new HashMap<>();

            for (int i = 0; i < N; i++) {
                T obj = factory.get();
                T exist = set.putIfAbsent(obj, obj);
                if (exist != null) {
                    if (comparator.compare(obj, exist) != 0)
                        return new Pair<>(obj, exist);
                    if (comparator.compare(exist, obj) != 0)
                        return new Pair<>(exist, obj);
                }
                if (comparator.compare(obj, obj) != 0)
                    return new Pair<>(obj, obj);
            }

            return null;
        }

        /**
         * Test condition if x is greater than y then y is less than x.
         *
         * @param factory
         * @param comparator
         * @return Return null if okay, or pair of values which make error if any.
         */
        Pair<T> testSymmetry(Supplier<T> factory, Comparator<T> comparator) {
            ArrayList<T> list = new ArrayList<>();

            for (int i = 0; i < N; i++) {
                list.add(factory.get());
            }

            for (int i = 0; i < N; i++)
                for (int j = 0; j < N; j++) {
                    int left = comparator.compare(list.get(i), list.get(j));
                    int right = comparator.compare(list.get(j), list.get(i));
                    if (!((left < 0 && right > 0) || (left > 0 && right < 0) || (left == 0 && right == 0)))
                        return new Pair<>(list.get(i), list.get(j));
                }

            return null;
        }

        /**
         * Class is used for show in assert.
         */
        static class Pair<T> {
            final T first;
            final T second;

            Pair(T first, T second) {
                this.first = first;
                this.second = second;
            }
        }
    }
}
