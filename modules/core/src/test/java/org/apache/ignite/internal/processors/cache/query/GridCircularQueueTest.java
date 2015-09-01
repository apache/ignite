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

package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayDeque;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 */
public class GridCircularQueueTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testQueue() {
        GridCacheQueryManager.CircularQueue<Integer> q = new GridCacheQueryManager.CircularQueue<>(4);

        ArrayDeque<Integer> d = new ArrayDeque<>();

        for (int i = 0; i < 10; i++) {
            q.add(i);
            d.add(i);
        }

        check(q, d);

        q.remove(4);
        remove(d, 4);

        check(q, d);

        for (int i = 100; i < 110; i++) {
            q.add(i);
            d.add(i);
        }

        check(q, d);

        int size = q.size();

        q.remove(size);
        remove(d, size);

        check(q, d);

        assertEquals(0, q.size());

        GridRandom rnd = new GridRandom();

        for (int i = 0; i < 15000; i++) {
            switch (rnd.nextInt(2)) {
                case 1:
                    if (q.size() > 0) {
                        int cnt = 1;

                        if (q.size() > 1)
                            cnt += rnd.nextInt(q.size() - 1);

                        q.remove(cnt);
                        remove(d, cnt);

                        break;
                    }

                case 0:
                    int cnt = rnd.nextInt(50);

                    for (int j = 0; j < cnt; j++) {
                        int x = rnd.nextInt();

                        q.add(x);
                        d.add(x);
                    }

                    break;
            }

            check(q, d);
        }
    }

    /**
     * @param d Deque.
     * @param n Number of elements.
     */
    private void remove(ArrayDeque<?> d, int n) {
        for (int i = 0; i < n; i++)
            assertNotNull(d.poll());
    }

    /**
     * @param q Queue.
     * @param d Dequeue.
     */
    private void check(GridCacheQueryManager.CircularQueue<?> q, ArrayDeque<?> d) {
        assertEquals(q.size(), d.size());

        int i = 0;

        for (Object o : d)
            assertEquals(q.get(i++), o);
    }
}