/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import junit.framework.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Test for {@link GridRandom}.
 */
public class GridRandomSelfTest extends TestCase {
    /**
     */
    public void testRandom() {
        for (int i = 0; i < 100; i++) {
            long seed = ThreadLocalRandom.current().nextLong();

            Random rnd1 = new Random(seed);
            Random rnd2 = new GridRandom(seed);

            for (int j = 1; j < 100000; j++) {
                assertEquals(rnd1.nextInt(), rnd2.nextInt());
                assertEquals(rnd1.nextInt(j), rnd2.nextInt(j));
                assertEquals(rnd1.nextLong(), rnd2.nextLong());
                assertEquals(rnd1.nextBoolean(), rnd2.nextBoolean());

                if (j % 1000 == 0) {
                    seed = ThreadLocalRandom.current().nextLong();

                    rnd1.setSeed(seed);
                    rnd2.setSeed(seed);
                }
            }
        }
    }

    /**
     * Test performance difference.
     */
    public void _testPerformance() {
        Random rnd = new GridRandom(); // new Random();

        long start = System.nanoTime();

        for (int i = 0; i < 2000000000; i++)
            rnd.nextInt();

        X.println("Time: " + (System.nanoTime() - start) + " ns");
    }
}
