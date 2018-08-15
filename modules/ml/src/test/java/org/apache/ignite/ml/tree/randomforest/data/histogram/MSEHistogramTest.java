package org.apache.ignite.ml.tree.randomforest.data.histogram;

import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.randomforest.data.BaggedVector;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class MSEHistogramTest {
    private BucketMeta feature1Meta = new BucketMeta(new FeatureMeta(0, true));
    private BucketMeta feature2Meta = new BucketMeta(new FeatureMeta(1, false));

    @Before
    public void setUp() throws Exception {
        feature2Meta.setMinValue(-5);
        feature2Meta.setBucketSize(1);
    }

    @Test
    public void testAdd() {

    }

    private BaggedVector[] dataset = new BaggedVector[] {
        new BaggedVector(VectorUtils.of(1, -1), 1, new int[] {1, 2}),
        new BaggedVector(VectorUtils.of(1, 2), 2, new int[] {2, 1}),
        new BaggedVector(VectorUtils.of(0, 3), 3, new int[] {2, 0}),
        new BaggedVector(VectorUtils.of(0, 1), 4, new int[] {2, 1}),
        new BaggedVector(VectorUtils.of(1, -4), 5, new int[] {1, 2}),
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
