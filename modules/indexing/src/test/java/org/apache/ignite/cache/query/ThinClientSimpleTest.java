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

package org.apache.ignite.cache.query;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.platform.client.ClientMessageParser;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.client.Config.SERVER;

/** */
public class ThinClientSimpleTest extends GridCommonAbstractTest {

    public static final byte[] VAL = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    /** */
    @Test
    public void testThinClientPerf() throws Exception {
        try (IgniteEx srv = startGrid()) {
            IgniteCache<Object, Object> c = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

            IntStream.range(0, 1000).forEach(i -> c.put(i, VAL));

            try (IgniteClient cln = Ignition.startClient(new ClientConfiguration().setAddresses(SERVER))) {
                ClientCache<Integer, byte[]> cc = cln.cache(DEFAULT_CACHE_NAME);

                ThreadLocalRandom r = ThreadLocalRandom.current();

                // Warmup
                for (int i = 0; i < 100_000; i++)
                    assertNotNull(cc.get(r.nextInt(1000)));

                for (boolean direct: new boolean[] {false, true}) {
                    ClientMessageParser.USE_DIRECT_READ = direct;

                    long start = System.nanoTime();

                    for (int i = 0; i < 100_000; i++)
                        assertTrue(Arrays.equals(VAL, cc.get(r.nextInt(1000))));

                    long finish = System.nanoTime();

                    Duration t = Duration.of(finish - start, ChronoUnit.NANOS);

                    System.out.println("time (millis) [direct=" + direct + "] = " + t.toMillis());
                }
            }
        }
    }
}
