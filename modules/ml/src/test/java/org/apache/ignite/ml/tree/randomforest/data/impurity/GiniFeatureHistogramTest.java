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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.dataset.feature.BucketMeta;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** */
public class GiniFeatureHistogramTest extends ImpurityHistogramTest {
    /** Feature 1 meta. */
    private BucketMeta feature1Meta = new BucketMeta(new FeatureMeta("", 0, true));
    /** Feature 2 meta. */
    private BucketMeta feature2Meta = new BucketMeta(new FeatureMeta("", 1, false));
    /** Feature 3 meta. */
    private BucketMeta feature3Meta = new BucketMeta(new FeatureMeta("", 2, true));

    /** */
    @Before
    public void setUp() throws Exception {
        feature2Meta.setMinVal(-5);
        feature2Meta.setBucketSize(1);
    }

    /** */
    @Test
    public void testAddVector() {
        Map<Double, Integer> lblMapping = new HashMap<>();
        lblMapping.put(1.0, 0);
        lblMapping.put(2.0, 1);
        lblMapping.put(3.0, 2);

        GiniHistogram catFeatureSmpl1 = new GiniHistogram(0, lblMapping, feature1Meta);
        GiniHistogram catFeatureSmpl2 = new GiniHistogram(1, lblMapping, feature1Meta);

        GiniHistogram contFeatureSmpl1 = new GiniHistogram(0, lblMapping, feature2Meta);
        GiniHistogram contFeatureSmpl2 = new GiniHistogram(1, lblMapping, feature2Meta);

        for (BootstrappedVector vec : dataset) {
            catFeatureSmpl1.addElement(vec);
            catFeatureSmpl2.addElement(vec);
            contFeatureSmpl1.addElement(vec);
            contFeatureSmpl2.addElement(vec);
        }

        checkBucketIds(catFeatureSmpl1.buckets(), new Integer[] {0, 1});
        checkBucketIds(catFeatureSmpl2.buckets(), new Integer[] {0, 1});
        checkBucketIds(contFeatureSmpl1.buckets(), new Integer[] {1, 4, 6, 7, 8});
        checkBucketIds(contFeatureSmpl2.buckets(), new Integer[] {1, 4, 6, 7, 8});

        //categorical feature
        checkCounters(catFeatureSmpl1.getHistForLabel(1.0), new double[] {2, 1}); //for feature values 0 and 1
        checkBucketIds(catFeatureSmpl1.getHistForLabel(1.0).buckets(), new Integer[] {0, 1});
        checkCounters(catFeatureSmpl1.getHistForLabel(2.0), new double[] {3});    //for feature value 1
        checkBucketIds(catFeatureSmpl1.getHistForLabel(2.0).buckets(), new Integer[] {1});
        checkCounters(catFeatureSmpl1.getHistForLabel(3.0), new double[] {2});    //for feature value 0
        checkBucketIds(catFeatureSmpl1.getHistForLabel(3.0).buckets(), new Integer[] {0});

        checkCounters(catFeatureSmpl2.getHistForLabel(1.0), new double[] {1, 2}); //for feature values 0 and 1
        checkBucketIds(catFeatureSmpl2.getHistForLabel(1.0).buckets(), new Integer[] {0, 1});
        checkCounters(catFeatureSmpl2.getHistForLabel(2.0), new double[] {3});    //for feature value 1
        checkBucketIds(catFeatureSmpl2.getHistForLabel(2.0).buckets(), new Integer[] {1});
        checkCounters(catFeatureSmpl2.getHistForLabel(3.0), new double[] {0});    //for feature value 0
        checkBucketIds(catFeatureSmpl2.getHistForLabel(3.0).buckets(), new Integer[] {0});

        //continuous feature
        checkCounters(contFeatureSmpl1.getHistForLabel(1.0), new double[] {1, 2}); //for feature values 0 and 1
        checkBucketIds(contFeatureSmpl1.getHistForLabel(1.0).buckets(), new Integer[] {4, 6});
        checkCounters(contFeatureSmpl1.getHistForLabel(2.0), new double[] {1, 2});    //for feature value 1
        checkBucketIds(contFeatureSmpl1.getHistForLabel(2.0).buckets(), new Integer[] {1, 7});
        checkCounters(contFeatureSmpl1.getHistForLabel(3.0), new double[] {2});    //for feature value 0
        checkBucketIds(contFeatureSmpl1.getHistForLabel(3.0).buckets(), new Integer[] {8});

        checkCounters(contFeatureSmpl2.getHistForLabel(1.0), new double[] {2, 1}); //for feature values 0 and 1
        checkBucketIds(contFeatureSmpl2.getHistForLabel(1.0).buckets(), new Integer[] {4, 6});
        checkCounters(contFeatureSmpl2.getHistForLabel(2.0), new double[] {2, 1});    //for feature value 1
        checkBucketIds(contFeatureSmpl2.getHistForLabel(2.0).buckets(), new Integer[] {1, 7});
        checkCounters(contFeatureSmpl2.getHistForLabel(3.0), new double[] {0});    //for feature value 0
        checkBucketIds(contFeatureSmpl2.getHistForLabel(3.0).buckets(), new Integer[] {8});
    }

    /** */
    @Test
    public void testSplit() {
        Map<Double, Integer> lblMapping = new HashMap<>();
        lblMapping.put(1.0, 0);
        lblMapping.put(2.0, 1);

        GiniHistogram catFeatureSmpl1 = new GiniHistogram(0, lblMapping, feature1Meta);
        GiniHistogram contFeatureSmpl1 = new GiniHistogram(0, lblMapping, feature2Meta);
        GiniHistogram emptyHist = new GiniHistogram(0, lblMapping, feature3Meta);
        GiniHistogram catFeatureSmpl2 = new GiniHistogram(0, lblMapping, feature3Meta);

        feature2Meta.setMinVal(-5);
        feature2Meta.setBucketSize(1);

        for (BootstrappedVector vec : toSplitDataset) {
            catFeatureSmpl1.addElement(vec);
            contFeatureSmpl1.addElement(vec);
            catFeatureSmpl2.addElement(vec);
        }

        NodeSplit catSplit = catFeatureSmpl1.findBestSplit().get();
        NodeSplit contSplit = contFeatureSmpl1.findBestSplit().get();
        assertEquals(1.0, catSplit.getValue(), 0.01);
        assertEquals(-0.5, contSplit.getValue(), 0.01);
        assertFalse(emptyHist.findBestSplit().isPresent());
        assertFalse(catFeatureSmpl2.findBestSplit().isPresent());
    }

    @Test
    public void testOfSums() {
        int sampleId = 0;
        BucketMeta bucketMeta1 = new BucketMeta(new FeatureMeta("", 0, false));
        bucketMeta1.setMinVal(0.);
        bucketMeta1.setBucketSize(0.1);
        BucketMeta bucketMeta2 = new BucketMeta(new FeatureMeta("", 1, true));

        GiniHistogram forAllHist1 = new GiniHistogram(sampleId, lblMapping, bucketMeta1);
        GiniHistogram forAllHist2 = new GiniHistogram(sampleId, lblMapping, bucketMeta2);

        List<GiniHistogram> partitions1 = new ArrayList<>();
        List<GiniHistogram> partitions2 = new ArrayList<>();
        int countOfPartitions = rnd.nextInt(1000);
        for(int i = 0; i < countOfPartitions; i++) {
            partitions1.add(new GiniHistogram(sampleId,lblMapping, bucketMeta1));
            partitions2.add(new GiniHistogram(sampleId,lblMapping, bucketMeta2));
        }

        int datasetSize = rnd.nextInt(10000);
        for(int i = 0; i < datasetSize; i++) {
            BootstrappedVector vec = randomVector(2, 1, true);
            vec.features().set(1, (vec.features().get(1) * 100) % 100);

            forAllHist1.addElement(vec);
            forAllHist2.addElement(vec);
            int partitionId = rnd.nextInt(countOfPartitions);
            partitions1.get(partitionId).addElement(vec);
            partitions2.get(partitionId).addElement(vec);
        }

        checkSums(forAllHist1, partitions1);
        checkSums(forAllHist2, partitions2);

        GiniHistogram emptyHist1 = new GiniHistogram(sampleId, lblMapping, bucketMeta1);
        GiniHistogram emptyHist2 = new GiniHistogram(sampleId, lblMapping, bucketMeta2);
        assertTrue(forAllHist1.isEqualTo(forAllHist1.plus(emptyHist1)));
        assertTrue(forAllHist2.isEqualTo(forAllHist2.plus(emptyHist2)));
        assertTrue(forAllHist1.isEqualTo(emptyHist1.plus(forAllHist1)));
        assertTrue(forAllHist2.isEqualTo(emptyHist2.plus(forAllHist2)));
    }

    /** */
    @Test
    public void testJoin() {
        Map<Double, Integer> lblMapping = new HashMap<>();
        lblMapping.put(1.0, 0);
        lblMapping.put(2.0, 1);
        lblMapping.put(3.0, 2);

        GiniHistogram catFeatureSmpl1 = new GiniHistogram(0, lblMapping, feature1Meta);
        GiniHistogram catFeatureSmpl2 = new GiniHistogram(0, lblMapping, feature1Meta);

        GiniHistogram contFeatureSmpl1 = new GiniHistogram(0, lblMapping, feature2Meta);
        GiniHistogram contFeatureSmpl2 = new GiniHistogram(0, lblMapping, feature2Meta);

        for (BootstrappedVector vec : dataset) {
            catFeatureSmpl1.addElement(vec);
            contFeatureSmpl1.addElement(vec);
        }

        for (BootstrappedVector vec : toSplitDataset) {
            catFeatureSmpl2.addElement(vec);
            contFeatureSmpl2.addElement(vec);
        }

        GiniHistogram res1 = catFeatureSmpl1.plus(catFeatureSmpl2);
        GiniHistogram res2 = contFeatureSmpl1.plus(contFeatureSmpl2);

        checkBucketIds(res1.buckets(), new Integer[] {0, 1, 2});
        checkBucketIds(res2.buckets(), new Integer[] {1, 4, 6, 7, 8});

        //categorical feature
        checkCounters(res1.getHistForLabel(1.0), new double[] {3, 2, 6}); //for feature values 0 and 1
        checkBucketIds(res1.getHistForLabel(1.0).buckets(), new Integer[] {0, 1, 2});
        checkCounters(res1.getHistForLabel(2.0), new double[] {4, 6});    //for feature value 1
        checkBucketIds(res1.getHistForLabel(2.0).buckets(), new Integer[] {0, 1});
        checkCounters(res1.getHistForLabel(3.0), new double[] {2});    //for feature value 0
        checkBucketIds(res1.getHistForLabel(3.0).buckets(), new Integer[] {0});

        //continuous feature
        checkCounters(res2.getHistForLabel(1.0), new double[] {1, 1, 8, 1}); //for feature values 0 and 1
        checkBucketIds(res2.getHistForLabel(1.0).buckets(), new Integer[] {1, 4, 6, 8});
        checkCounters(res2.getHistForLabel(2.0), new double[] {1, 4, 0, 5});    //for feature value 1
        checkBucketIds(res2.getHistForLabel(2.0).buckets(), new Integer[] {1, 4, 6, 7});
        checkCounters(res2.getHistForLabel(3.0), new double[] {2});    //for feature value 0
        checkBucketIds(res2.getHistForLabel(3.0).buckets(), new Integer[] {8});
    }

    /** Dataset. */
    private BootstrappedVector[] dataset = new BootstrappedVector[] {
        new BootstrappedVector(VectorUtils.of(1, -1), 1, new int[] {1, 2}),
        new BootstrappedVector(VectorUtils.of(1, 2), 2, new int[] {2, 1}),
        new BootstrappedVector(VectorUtils.of(0, 3), 3, new int[] {2, 0}),
        new BootstrappedVector(VectorUtils.of(0, 1), 1, new int[] {2, 1}),
        new BootstrappedVector(VectorUtils.of(1, -4), 2, new int[] {1, 2}),
    };

    /** To split dataset. */
    private BootstrappedVector[] toSplitDataset = new BootstrappedVector[] {
        new BootstrappedVector(VectorUtils.of(0, -1, 0, 0), 2, new int[] {2}),
        new BootstrappedVector(VectorUtils.of(0, -1, 0, 0), 2, new int[] {1}),
        new BootstrappedVector(VectorUtils.of(0, -1, 0, 0), 2, new int[] {1}),
        new BootstrappedVector(VectorUtils.of(0, 3, 0, 0), 1, new int[] {1}),
        new BootstrappedVector(VectorUtils.of(0, 1, 0, 0), 2, new int[] {0}),
        new BootstrappedVector(VectorUtils.of(1, 2, 0, 0), 2, new int[] {1}),
        new BootstrappedVector(VectorUtils.of(1, 2, 0, 0), 2, new int[] {1}),
        new BootstrappedVector(VectorUtils.of(1, 2, 0, 0), 2, new int[] {1}),
        new BootstrappedVector(VectorUtils.of(1, -4, 0, 0), 1, new int[] {1}),
        new BootstrappedVector(VectorUtils.of(2, 1, 0, 0), 1, new int[] {1}),
        new BootstrappedVector(VectorUtils.of(2, 1, 0, 0), 1, new int[] {1}),
        new BootstrappedVector(VectorUtils.of(2, 1, 0, 0), 1, new int[] {1}),
        new BootstrappedVector(VectorUtils.of(2, 1, 0, 0), 1, new int[] {1}),
        new BootstrappedVector(VectorUtils.of(2, 1, 0, 0), 1, new int[] {1}),
        new BootstrappedVector(VectorUtils.of(2, 1, 0, 1), 1, new int[] {1}),
    };
}
