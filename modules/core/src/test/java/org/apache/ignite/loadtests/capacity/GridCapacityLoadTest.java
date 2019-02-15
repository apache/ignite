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

package org.apache.ignite.loadtests.capacity;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Continuous mapper load test.
 */
public class GridCapacityLoadTest {
    /** Heap usage. */
    private static final MemoryMXBean mem = ManagementFactory.getMemoryMXBean();

    /**
     * Main method.
     *
     * @param args Parameters.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        // Initialize Spring factory.
        ClassPathXmlApplicationContext ctx =
            new ClassPathXmlApplicationContext("org/apache/ignite/loadtests/capacity/spring-capacity-cache.xml");

        IgniteConfiguration cfg = (IgniteConfiguration)ctx.getBean("grid.cfg");

        try (Ignite g = G.start(cfg)) {
            IgniteCache<Integer, Integer> c = g.cache("test-cache");

            long init = mem.getHeapMemoryUsage().getUsed();

            printHeap(init);

            int cnt = 0;

            for (; cnt < 3000000; cnt++) {
                c.put(cnt, cnt);

                if (cnt % 10000 == 0) {
                    X.println("Stored count: " + cnt);

                    printHeap(init);

                    if (cnt > 2100000 &&  cnt % 100000 == 0)
                        System.gc();
                }
            }

            System.gc();

            Thread.sleep(1000);

            printHeap(init);

            MemoryUsage heap = mem.getHeapMemoryUsage();

            long used = heap.getUsed() - init;

            long entrySize = cnt > 0 ? used / cnt : 0;

            X.println("Average entry size: " + entrySize);
        }
    }

    private static void printHeap(long init) {
        MemoryUsage heap = mem.getHeapMemoryUsage();

        long max = heap.getMax() - init;
        long used = heap.getUsed() - init;
        long left = max - used;

        X.println("Heap left: " + (left / (1024 * 1024)) + "MB");
    }
}