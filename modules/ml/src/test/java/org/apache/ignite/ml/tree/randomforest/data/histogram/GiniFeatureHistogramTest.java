package org.apache.ignite.ml.tree.randomforest.data.histogram;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.randomforest.data.BaggedVector;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GiniFeatureHistogramTest extends ImpurityHistogramTest {
    private BucketMeta feature1Meta = new BucketMeta(new FeatureMeta(0, true));
    private BucketMeta feature2Meta = new BucketMeta(new FeatureMeta(1, false));

    @Before
    public void setUp() throws Exception {
        feature2Meta.setMinValue(-5);
        feature2Meta.setBucketSize(1);
    }

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

        for (BaggedVector vec : dataset) {
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

    @Test
    public void testSplit() {
        Map<Double, Integer> lblMapping = new HashMap<>();
        lblMapping.put(1.0, 0);
        lblMapping.put(2.0, 1);

        GiniHistogram catFeatureSmpl1 = new GiniHistogram(0, lblMapping, feature1Meta);
        GiniHistogram contFeatureSmpl1 = new GiniHistogram(0, lblMapping, feature2Meta);

        feature2Meta.setMinValue(-5);
        feature2Meta.setBucketSize(1);

        for (BaggedVector vec : toSplitDataset) {
            catFeatureSmpl1.addElement(vec);
            contFeatureSmpl1.addElement(vec);
        }

        NodeSplit catSplit = catFeatureSmpl1.findBestSplit();
        NodeSplit contSplit = contFeatureSmpl1.findBestSplit();
        assertEquals(0.0, catSplit.getValue(), 0.01);
        assertEquals(-0.5, contSplit.getValue(), 0.01);
    }

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

        for (BaggedVector vec : dataset) {
            catFeatureSmpl1.addElement(vec);
            contFeatureSmpl1.addElement(vec);
        }

        for (BaggedVector vec : toSplitDataset) {
            catFeatureSmpl2.addElement(vec);
            contFeatureSmpl2.addElement(vec);
        }

        catFeatureSmpl1.addHist(catFeatureSmpl2);
        contFeatureSmpl1.addHist(contFeatureSmpl2);

        checkBucketIds(catFeatureSmpl1.buckets(), new Integer[] {0, 1, 2});
        checkBucketIds(contFeatureSmpl1.buckets(), new Integer[] {1, 4, 6, 7, 8});

        //categorical feature
        checkCounters(catFeatureSmpl1.getHistForLabel(1.0), new double[] {7, 2, 1}); //for feature values 0 and 1
        checkBucketIds(catFeatureSmpl1.getHistForLabel(1.0).buckets(), new Integer[] {0, 1, 2});
        checkCounters(catFeatureSmpl1.getHistForLabel(2.0), new double[] {0, 6, 5});    //for feature value 1
        checkBucketIds(catFeatureSmpl1.getHistForLabel(2.0).buckets(), new Integer[] {0, 1, 2});
        checkCounters(catFeatureSmpl1.getHistForLabel(3.0), new double[] {2});    //for feature value 0
        checkBucketIds(catFeatureSmpl1.getHistForLabel(3.0).buckets(), new Integer[] {0});

        //continuous feature
        checkCounters(contFeatureSmpl1.getHistForLabel(1.0), new double[] {1, 5, 3, 1}); //for feature values 0 and 1
        checkBucketIds(contFeatureSmpl1.getHistForLabel(1.0).buckets(), new Integer[] {1, 4, 6, 8});
        checkCounters(contFeatureSmpl1.getHistForLabel(2.0), new double[] {1, 5, 5});    //for feature value 1
        checkBucketIds(contFeatureSmpl1.getHistForLabel(2.0).buckets(), new Integer[] {1, 6, 7});
        checkCounters(contFeatureSmpl1.getHistForLabel(3.0), new double[] {2});    //for feature value 0
        checkBucketIds(contFeatureSmpl1.getHistForLabel(3.0).buckets(), new Integer[] {8});
    }

    private BaggedVector[] dataset = new BaggedVector[] {
        new BaggedVector(VectorUtils.of(1, -1), 1, new int[] {1, 2}),
        new BaggedVector(VectorUtils.of(1, 2), 2, new int[] {2, 1}),
        new BaggedVector(VectorUtils.of(0, 3), 3, new int[] {2, 0}),
        new BaggedVector(VectorUtils.of(0, 1), 1, new int[] {2, 1}),
        new BaggedVector(VectorUtils.of(1, -4), 2, new int[] {1, 2}),
    };

    private BaggedVector[] toSplitDataset = new BaggedVector[] {
        new BaggedVector(VectorUtils.of(0, -1), 1, new int[] {2}),
        new BaggedVector(VectorUtils.of(0, -1), 1, new int[] {1}),
        new BaggedVector(VectorUtils.of(0, -1), 1, new int[] {1}),
        new BaggedVector(VectorUtils.of(0, 3), 1, new int[] {1}),
        new BaggedVector(VectorUtils.of(0, 1), 2, new int[] {0}),
        new BaggedVector(VectorUtils.of(1, 2), 2, new int[] {1}),
        new BaggedVector(VectorUtils.of(1, 2), 2, new int[] {1}),
        new BaggedVector(VectorUtils.of(1, 2), 2, new int[] {1}),
        new BaggedVector(VectorUtils.of(1, -4), 1, new int[] {1}),
        new BaggedVector(VectorUtils.of(2, 1), 2, new int[] {1}),
        new BaggedVector(VectorUtils.of(2, 1), 2, new int[] {1}),
        new BaggedVector(VectorUtils.of(2, 1), 2, new int[] {1}),
        new BaggedVector(VectorUtils.of(2, 1), 2, new int[] {1}),
        new BaggedVector(VectorUtils.of(2, 1), 2, new int[] {1}),
        new BaggedVector(VectorUtils.of(2, 1), 1, new int[] {1}),
    };
}
