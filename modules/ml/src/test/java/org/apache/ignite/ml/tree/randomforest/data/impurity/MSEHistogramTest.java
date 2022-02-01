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

package org.apache.ignite.ml.tree.randomforest.data.impurity;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.ml.dataset.feature.BucketMeta;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** */
public class MSEHistogramTest extends ImpurityHistogramTest {
    /** Feature 1 meta. */
    private BucketMeta feature1Meta = new BucketMeta(new FeatureMeta("", 0, true));

    /** Feature 2 meta. */
    private BucketMeta feature2Meta = new BucketMeta(new FeatureMeta("", 1, false));

    /** */
    @Before
    public void setUp() throws Exception {
        feature2Meta.setMinVal(-5);
        feature2Meta.setBucketSize(1);
    }

    /** */
    @Test
    public void testAdd() {
        MSEHistogram catHist1 = new MSEHistogram(0, feature1Meta);
        MSEHistogram contHist1 = new MSEHistogram(0, feature2Meta);

        MSEHistogram catHist2 = new MSEHistogram(1, feature1Meta);
        MSEHistogram contHist2 = new MSEHistogram(1, feature2Meta);

        for (BootstrappedVector vec : dataset) {
            catHist1.addElement(vec);
            catHist2.addElement(vec);
            contHist1.addElement(vec);
            contHist2.addElement(vec);
        }

        checkBucketIds(catHist1.buckets(), new Integer[] {0, 1});
        checkBucketIds(catHist2.buckets(), new Integer[] {0, 1});
        checkBucketIds(contHist1.buckets(), new Integer[] {1, 4, 6, 7, 8});
        checkBucketIds(contHist2.buckets(), new Integer[] {1, 4, 6, 7, 8});

        //counters
        checkCounters(catHist1.getCounters(), new double[] {4, 4});
        checkCounters(catHist2.getCounters(), new double[] {1, 5});
        checkCounters(contHist1.getCounters(), new double[] {1, 1, 2, 2, 2});
        checkCounters(contHist2.getCounters(), new double[] {2, 2, 1, 1, 0});

        //ys
        checkCounters(catHist1.getSumOfLabels(), new double[] {2 * 4 + 2 * 3, 5 + 1 + 2 * 2});
        checkCounters(catHist2.getSumOfLabels(), new double[] {4, 2 * 5 + 2 * 1 + 2});
        checkCounters(contHist1.getSumOfLabels(), new double[] {5 * 1, 1 * 1, 4 * 2, 2 * 2, 3 * 2});
        checkCounters(contHist2.getSumOfLabels(), new double[]{ 2 * 5, 2 * 1, 1 * 4, 2 * 1, 0 * 3 });

        //y2s
        checkCounters(catHist1.getSumOfSquaredLabels(), new double[] {2 * 4 * 4 + 2 * 3 * 3, 5 * 5 + 1 + 2 * 2 * 2});
        checkCounters(catHist2.getSumOfSquaredLabels(), new double[] {4 * 4, 2 * 5 * 5 + 2 * 1 * 1 + 2 * 2});
        checkCounters(contHist1.getSumOfSquaredLabels(), new double[] {1 * 5 * 5, 1 * 1 * 1, 2 * 4 * 4, 2 * 2 * 2, 2 * 3 * 3});
        checkCounters(contHist2.getSumOfSquaredLabels(), new double[]{ 2 * 5 * 5, 2 * 1 * 1, 1 * 4 * 4, 1 * 2 * 2, 0 * 3 * 3 });
    }

    /** */
    @Test
    public void testOfSums() {
        int sampleId = 0;
        BucketMeta bucketMeta1 = new BucketMeta(new FeatureMeta("", 0, false));
        bucketMeta1.setMinVal(0.);
        bucketMeta1.setBucketSize(0.1);
        BucketMeta bucketMeta2 = new BucketMeta(new FeatureMeta("", 1, true));

        MSEHistogram forAllHist1 = new MSEHistogram(sampleId, bucketMeta1);
        MSEHistogram forAllHist2 = new MSEHistogram(sampleId, bucketMeta2);

        List<MSEHistogram> partitions1 = new ArrayList<>();
        List<MSEHistogram> partitions2 = new ArrayList<>();

        int cntOfPartitions = rnd.nextInt(100) + 1;

        for (int i = 0; i < cntOfPartitions; i++) {
            partitions1.add(new MSEHistogram(sampleId, bucketMeta1));
            partitions2.add(new MSEHistogram(sampleId, bucketMeta2));
        }

        int datasetSize = rnd.nextInt(1000) + 1;
        for (int i = 0; i < datasetSize; i++) {
            BootstrappedVector vec = randomVector(false);
            vec.features().set(1, (vec.features().get(1) * 100) % 100);

            forAllHist1.addElement(vec);
            forAllHist2.addElement(vec);
            int partId = rnd.nextInt(cntOfPartitions);
            partitions1.get(partId).addElement(vec);
            partitions2.get(partId).addElement(vec);
        }

        checkSums(forAllHist1, partitions1);
        checkSums(forAllHist2, partitions2);

        MSEHistogram emptyHist1 = new MSEHistogram(sampleId, bucketMeta1);
        MSEHistogram emptyHist2 = new MSEHistogram(sampleId, bucketMeta2);
        assertTrue(forAllHist1.isEqualTo(forAllHist1.plus(emptyHist1)));
        assertTrue(forAllHist2.isEqualTo(forAllHist2.plus(emptyHist2)));
        assertTrue(forAllHist1.isEqualTo(emptyHist1.plus(forAllHist1)));
        assertTrue(forAllHist2.isEqualTo(emptyHist2.plus(forAllHist2)));
    }

    /** Dataset. */
    private BootstrappedVector[] dataset = new BootstrappedVector[] {
        new BootstrappedVector(VectorUtils.of(1, -4), 5, new int[] {1, 2}),
        new BootstrappedVector(VectorUtils.of(1, -1), 1, new int[] {1, 2}),
        new BootstrappedVector(VectorUtils.of(0, 1), 4, new int[] {2, 1}),
        new BootstrappedVector(VectorUtils.of(1, 2), 2, new int[] {2, 1}),
        new BootstrappedVector(VectorUtils.of(0, 3), 3, new int[] {2, 0}),
    };
}
