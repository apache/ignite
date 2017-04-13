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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.tcp.internal.RegionNodeComparator;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.atomic.AtomicLong;

/** Tests for class RegionNodeComparator. */
public class RegionNodeComparatorTest extends GridCommonAbstractTest {
    /**
     * The test for the preservation of order.
     *
     * @throws Exception If failed.
     */
    public void testSaveOrdering() throws Exception {
        final Comparator<TcpDiscoveryNode> comparator = new RegionNodeComparator();
        final NodeFactory factory = new NodeFactory();

        ArrayList<TcpDiscoveryNode> allNodes = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            allNodes.add(factory.get(1L));
        }

        TreeSet<TcpDiscoveryNode> nativeSet = new TreeSet<>();
        TreeSet<TcpDiscoveryNode> customSet = new TreeSet<>(comparator);

        for (int i = 0; i < allNodes.size(); i++) {
            nativeSet.add(allNodes.get(i));
            customSet.add(allNodes.get(allNodes.size() - i - 1));
        }

        assertEqualsCollections(nativeSet, customSet);
    }

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
     * Convert incorrect compared nodes to debug string.
     *
     * @param pair Pair of nodes.
     * @return Debug string.
     */
    private static String pairToString(ComparatorTester.Pair<TcpDiscoveryNode> pair) {
        return "[firstNodeInternalOrder=" + pair.first.internalOrder() +
            ", firstNodeRegionId=" + pair.first.getClusterRegionId() +
            ", secondNodeInternalOrder=" + pair.second.internalOrder() +
            ", secondNodeRegionId=" + pair.second.getClusterRegionId() + ']';
    }

    /**
     * Test for correct sorting
     *
     * @throws Exception If failed.
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
        Collections.sort(sortedNodes, comparator);

        int i = 0;
        for (int r = 1; r <= REGIONS; r++) {
            for (int j = 0; j < N / REGIONS; j++) {
                assertEquals(r == REGIONS ? 0 : r, sortedNodes.get(i).internalOrder() % REGIONS);
                i++;
            }
        }
    }

    /** Build nodes with correct ids and cluster region ids. */
    private static class NodeFactory implements Supplier<TcpDiscoveryNode> {
        /**  */
        final AtomicLong id = new AtomicLong(1L);
        /** */
        final Random random = new Random();

        /**
         * Return node with sequential id and random cluster region id.
         *
         * @return Node with sequential id and random cluster region id.
         */
        @Override
        public TcpDiscoveryNode get() {
            TcpDiscoveryNode node = get(random.nextInt(10));
            return node;
        }

        /**
         * Return node with sequential id and specific cluster region id.
         *
         * @param region Cluster region id.
         * @return Node with sequential id and specific cluster region id.
         */
        public TcpDiscoveryNode get(long region) {

            TcpDiscoveryNode node = new TcpDiscoveryNode(new UUID(0L, id.getAndIncrement()),
                Collections.singletonList("1.1.1.1"), Collections.singletonList("1.1.1.1"), 88,
                new DiscoveryMetricsProvider() {
                private final Map<Integer, CacheMetrics> EMPTY = new HashMap<Integer, CacheMetrics>(0);

                @Override
                public ClusterMetrics metrics() {
                    return null;
                }

                @Override
                public Map<Integer, CacheMetrics> cacheMetrics() {
                    return EMPTY;
                }
            }, new IgniteProductVersion(), null, region);
            node.internalOrder(node.id().getLeastSignificantBits());
            return node;
        }

    }

    /** Class for testing comparators. In the future we may add new comparator, and then may use this class. */
    private static class ComparatorTester<T> {
        /** The number of experiments */
        final int N;

        /** */
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
                if (set.containsKey(obj)) {
                    T exist = set.get(obj);
                    if (comparator.compare(obj, exist) != 0)
                        return new Pair<>(obj, exist);
                    if (comparator.compare(exist, obj) != 0)
                        return new Pair<>(exist, obj);
                }
                else {
                    set.put(obj, obj);
                    if (comparator.compare(obj, obj) != 0)
                        return new Pair<>(obj, obj);
                }
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

        /** Class is used for show in assert. */
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
