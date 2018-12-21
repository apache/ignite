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
