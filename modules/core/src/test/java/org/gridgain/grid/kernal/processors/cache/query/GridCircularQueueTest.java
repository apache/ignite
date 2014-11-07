/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.util.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

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
