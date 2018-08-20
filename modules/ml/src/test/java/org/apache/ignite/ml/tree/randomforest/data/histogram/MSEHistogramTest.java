package org.apache.ignite.ml.tree.randomforest.data.histogram;

import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.randomforest.data.BaggedVector;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class MSEHistogramTest extends ImpurityHistogramTest {
    private BucketMeta feature1Meta = new BucketMeta(new FeatureMeta(0, true));
    private BucketMeta feature2Meta = new BucketMeta(new FeatureMeta(1, false));

    @Before
    public void setUp() throws Exception {
        feature2Meta.setMinValue(-5);
        feature2Meta.setBucketSize(1);
    }

    @Test
    public void testAdd() {
        MSEHistogram catHist1 = new MSEHistogram(0, feature1Meta);
        MSEHistogram contHist1 = new MSEHistogram(0, feature2Meta);

        MSEHistogram catHist2 = new MSEHistogram(1, feature1Meta);
        MSEHistogram contHist2 = new MSEHistogram(1, feature2Meta);

        for (BaggedVector vec : dataset) {
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
        checkCounters(catHist1.getYs(), new double[] {2 * 4 + 2 * 3, 5 + 1 + 2 * 2});
        checkCounters(catHist2.getYs(), new double[] {4, 2 * 5 + 2 * 1 + 2});
        checkCounters(contHist1.getYs(), new double[] {5 * 1, 1 * 1, 4 * 2, 2 * 2, 3 * 2});
        checkCounters(contHist2.getYs(), new double[]{ 2 * 5, 2 * 1, 1 * 4, 2 * 1, 0 * 3 });

        //y2s
        checkCounters(catHist1.getY2s(), new double[] {2 * 4 * 4 + 2 * 3 * 3, 5 * 5 + 1 + 2 * 2 * 2});
        checkCounters(catHist2.getY2s(), new double[] {4 * 4, 2 * 5 * 5 + 2 * 1 * 1 + 2 * 2});
        checkCounters(contHist1.getY2s(), new double[] {1 * 5 * 5, 1 * 1 * 1, 2 * 4 * 4, 2 * 2 * 2, 2 * 3 * 3});
        checkCounters(contHist2.getY2s(), new double[]{ 2 * 5 * 5, 2 * 1 * 1, 1 * 4 * 4, 1 * 2 * 2, 0 * 3 * 3 });
    }

    private BaggedVector[] dataset = new BaggedVector[] {
        new BaggedVector(VectorUtils.of(1, -4), 5, new int[] {1, 2}),
        new BaggedVector(VectorUtils.of(1, -1), 1, new int[] {1, 2}),
        new BaggedVector(VectorUtils.of(0, 1), 4, new int[] {2, 1}),
        new BaggedVector(VectorUtils.of(1, 2), 2, new int[] {2, 1}),
        new BaggedVector(VectorUtils.of(0, 3), 3, new int[] {2, 0}),
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
