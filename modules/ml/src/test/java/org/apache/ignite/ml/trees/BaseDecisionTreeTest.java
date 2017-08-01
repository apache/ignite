package org.apache.ignite.ml.trees;

import org.apache.ignite.Ignite;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledVectorDouble;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Arrays;
import java.util.PrimitiveIterator;
import java.util.stream.DoubleStream;

public class BaseDecisionTreeTest extends GridCommonAbstractTest {
    private static final int NODE_COUNT = 2;

    /** Grid instance. */
    protected Ignite ignite;

    /**
     * Default constructor.
     */
    public BaseDecisionTreeTest() {
        super(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(NODE_COUNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    static IgniteFunction<DoubleStream, Double> SIMPLE_VARIANCE_CALCULATOR = stream -> {
        PrimitiveIterator.OfDouble iter = stream.iterator();
        int i = 0;

        double mean = 0.0;
        double m2 = 0.0;

        while (iter.hasNext()) {
            i++;
            double x = iter.next();
            double delta = x - mean;
            mean += delta / i;
            double delta2 = x - mean;
            m2 += delta * delta2;
        }

        return i > 0 ? m2 / i : 0.0;
    };

    static LabeledVectorDouble<DenseLocalOnHeapVector> asLabeledVector(double arr[]) {
        return new LabeledVectorDouble<>(new DenseLocalOnHeapVector(Arrays.copyOf(arr, arr.length - 1)), arr[arr.length - 1]);
    }
}
