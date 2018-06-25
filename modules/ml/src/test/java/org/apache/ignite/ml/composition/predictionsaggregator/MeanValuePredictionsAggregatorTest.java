package org.apache.ignite.ml.composition.predictionsaggregator;

import org.junit.Test;

import static org.junit.Assert.*;

public class MeanValuePredictionsAggregatorTest {
    private PredictionsAggregator aggregator = new MeanValuePredictionsAggregator();

    /** */
    @Test public void testApply() {
        assertEquals(0.75, aggregator.apply(new double[]{1.0, 1.0, 1.0, 0.0}), 0.001);
    }
}
