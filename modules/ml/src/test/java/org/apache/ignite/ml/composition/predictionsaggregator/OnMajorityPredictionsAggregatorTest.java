package org.apache.ignite.ml.composition.predictionsaggregator;

import org.junit.Test;

import static org.junit.Assert.*;

public class OnMajorityPredictionsAggregatorTest {
    private PredictionsAggregator aggregator = new OnMajorityPredictionsAggregator();

    /** */
    @Test public void testApply() {
        assertEquals(1.0, aggregator.apply(new double[]{1.0, 1.0, 1.0, 0.0}), 0.001);
    }
}
