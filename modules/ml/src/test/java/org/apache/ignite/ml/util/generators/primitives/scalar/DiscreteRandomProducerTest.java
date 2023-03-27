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

package org.apache.ignite.ml.util.generators.primitives.scalar;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link DiscreteRandomProducer}.
 */
public class DiscreteRandomProducerTest {
    /** */
    @Test
    public void testGet() {
        double[] probs = new double[] {0.1, 0.2, 0.3, 0.4};
        DiscreteRandomProducer producer = new DiscreteRandomProducer(0L, probs);

        Map<Integer, Double> counters = new HashMap<>();
        IntStream.range(0, probs.length).forEach(i -> counters.put(i, 0.0));

        final int N = 500000;
        Stream.generate(producer::getInt).limit(N).forEach(i -> counters.put(i, counters.get(i) + 1));
        IntStream.range(0, probs.length).forEach(i -> counters.put(i, counters.get(i) / N));

        for (int i = 0; i < probs.length; i++)
            assertEquals(probs[i], counters.get(i), 0.01);

        assertEquals(probs.length, producer.size());
    }

    /** */
    @Test
    public void testSeedConsidering() {
        DiscreteRandomProducer producer1 = new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 0.4);
        DiscreteRandomProducer producer2 = new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 0.4);

        assertEquals(producer1.get(), producer2.get(), 0.0001);
    }

    /** */
    @Test
    public void testUniformGeneration() {
        int N = 10;
        DiscreteRandomProducer producer = DiscreteRandomProducer.uniform(N);

        Map<Integer, Double> counters = new HashMap<>();
        IntStream.range(0, N).forEach(i -> counters.put(i, 0.0));

        final int sampleSize = 500000;
        Stream.generate(producer::getInt).limit(sampleSize).forEach(i -> counters.put(i, counters.get(i) + 1));
        IntStream.range(0, N).forEach(i -> counters.put(i, counters.get(i) / sampleSize));

        for (int i = 0; i < N; i++)
            assertEquals(1.0 / N, counters.get(i), 0.01);
    }

    /** */
    @Test
    public void testDistributionGeneration() {
        double[] probs = DiscreteRandomProducer.randomDistribution(5, 0L);
        assertArrayEquals(new double[] {0.23, 0.27, 0.079, 0.19, 0.20}, probs, 0.01);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDistribution1() {
        new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 0.0);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDistribution2() {
        new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 1.0);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDistribution3() {
        new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 1.0, -0.6);
    }
}
