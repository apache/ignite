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

package org.apache.ignite.util;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import junit.framework.TestCase;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.X;

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
    public void testPerformance() {
        fail("https://issues.apache.org/jira/browse/IGNITE-824");

        Random rnd = new GridRandom(); // new Random();

        long start = System.nanoTime();

        for (int i = 0; i < 2000000000; i++)
            rnd.nextInt();

        X.println("Time: " + (System.nanoTime() - start) + " ns");
    }
}