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

package org.apache.ignite.ml.dataset.feature;

import java.util.Optional;
import java.util.TreeMap;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** */
public class ObjectHistogramTest {
    /** Data first partition. */
    private double[] dataFirstPart = new double[] {0., 0., 0., 0., 1., 1., 1, 2., 2., 3., 4., 5.};
    /** Data second partition. */
    private double[] dataSecondPart = new double[] {0., 1., 0., 1., 0., 1., 0, 1., 0., 1., 0., 5., 6.};

    /** */
    private ObjectHistogram<Double> hist1;
    /** */
    private ObjectHistogram<Double> hist2;

    /**
     *
     */
    @Before
    public void setUp() throws Exception {
        hist1 = new ObjectHistogram<>(this::computeBucket, x -> 1.);
        hist2 = new ObjectHistogram<>(this::computeBucket, x -> 1.);

        fillHist(hist1, dataFirstPart);
        fillHist(hist2, dataSecondPart);
    }

    /**
     * @param hist History.
     * @param data Data.
     */
    private void fillHist(ObjectHistogram<Double> hist, double[] data) {
        for (int i = 0; i < data.length; i++)
            hist.addElement(data[i]);
    }

    /**
     *
     */
    @Test
    public void testBuckets() {
        testBuckets(hist1, new int[] {0, 1, 2, 3, 4, 5}, new int[] {4, 3, 2, 1, 1, 1});
        testBuckets(hist2, new int[] {0, 1, 5, 6}, new int[] {6, 5, 1, 1});
    }

    /**
     * @param hist History.
     * @param expectedBuckets Expected buckets.
     * @param expectedCounters Expected counters.
     */
    private void testBuckets(ObjectHistogram<Double> hist, int[] expectedBuckets, int[] expectedCounters) {
        int size = hist.buckets().size();
        int[] buckets = new int[size];
        int[] counters = new int[size];
        int ptr = 0;
        for (int bucket : hist.buckets()) {
            counters[ptr] = hist.get(bucket).get().intValue();
            buckets[ptr++] = bucket;
        }

        assertArrayEquals(expectedBuckets, buckets);
        assertArrayEquals(expectedCounters, counters);
    }

    /**
     *
     */
    @Test
    public void testAdd() {
        double value = 100.;
        hist1.addElement(value);
        Optional<Double> counter = hist1.get(computeBucket(value));

        assertTrue(counter.isPresent());
        assertEquals(1, counter.get().intValue());
    }

    /**
     *
     */
    @Test
    public void testAddHist() {
        hist1.addHist(hist2);
        testBuckets(hist1, new int[] {0, 1, 2, 3, 4, 5, 6}, new int[] {10, 8, 2, 1, 1, 2, 1});
    }

    /**
     *
     */
    @Test
    public void testDistributionFunction() {
        TreeMap<Integer, Double> distribution = hist1.computeDistributionFunction();

        int[] buckets = new int[distribution.size()];
        double[] sums = new double[distribution.size()];

        int ptr = 0;
        for(int bucket : distribution.keySet()) {
            sums[ptr] = distribution.get(bucket);
            buckets[ptr++] = bucket;
        }

        assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5}, buckets);
        assertArrayEquals(new double[] {4., 7., 9., 10., 11., 12.}, sums, 0.01);
    }

    /**
     * @param value Value.
     */
    private int computeBucket(Double value) {
        return (int)Math.rint(value);
    }
}
