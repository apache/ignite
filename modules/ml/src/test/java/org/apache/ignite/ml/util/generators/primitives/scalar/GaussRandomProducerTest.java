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

import java.util.Random;
import java.util.stream.IntStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link GaussRandomProducer}.
 */
public class GaussRandomProducerTest {
    /** */
    @Test
    public void testGet() {
        Random random = new Random(0L);
        final double mean = random.nextInt(5) - 2.5;
        final double variance = random.nextInt(5);
        GaussRandomProducer producer = new GaussRandomProducer(mean, variance, 1L);

        final int N = 50000;
        double meanStat = IntStream.range(0, N).mapToDouble(i -> producer.get()).sum() / N;
        double varianceStat = IntStream.range(0, N).mapToDouble(i -> Math.pow(producer.get() - mean, 2)).sum() / N;

        assertEquals(mean, meanStat, 0.01);
        assertEquals(variance, varianceStat, 0.1);
    }

    /** */
    @Test
    public void testSeedConsidering() {
        GaussRandomProducer producer1 = new GaussRandomProducer(0L);
        GaussRandomProducer producer2 = new GaussRandomProducer(0L);

        assertEquals(producer1.get(), producer2.get(), 0.0001);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testIllegalVariance1() {
        new GaussRandomProducer(0, 0.);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testIllegalVariance2() {
        new GaussRandomProducer(0, -1.);
    }
}
