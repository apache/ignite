package de.bwaldvogel.mongo.backend.aggregation.accumulator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

public class MinAccumulatorTest {

    private final ComparingAccumulator accumulator = new MinAccumulator(null, null);

    @Test
    void testAccumulateNumbers() throws Exception {
        accumulator.aggregate(5);
        accumulator.aggregate(1);
        accumulator.aggregate(3);

        Object result = accumulator.getResult();
        assertThat(result).isEqualTo(1);
    }

    @Test
    void testAccumulateArrays() throws Exception {
        accumulator.aggregate(Arrays.asList(10, 20, 30));
        accumulator.aggregate(Arrays.asList(3, 40));
        accumulator.aggregate(Arrays.asList(11, 25));

        Object result = accumulator.getResult();
        assertThat(result).isEqualTo(Arrays.asList(3, 40));
    }

    @Test
    void testAccumulateArraysAndNonArray() throws Exception {
        accumulator.aggregate(Arrays.asList(3, 40));
        accumulator.aggregate(Arrays.asList(10, 20, 30));
        accumulator.aggregate(50);

        Object result = accumulator.getResult();
        assertThat(result).isEqualTo(50);
    }

    @Test
    void testAccumulateNothing() throws Exception {
        Object result = accumulator.getResult();
        assertThat(result).isNull();
    }

}
