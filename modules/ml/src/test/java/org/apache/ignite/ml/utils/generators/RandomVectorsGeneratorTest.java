package org.apache.ignite.ml.utils.generators;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.dataset.CirclesDatasetGenerator;
import org.junit.Test;

public class RandomVectorsGeneratorTest {
    @Test
    public void name() {
                Stream<LabeledVector<Vector, Double>> targetStream = new CirclesDatasetGenerator(5, 5, 5).labeled();
//        Stream<LabeledVector<Vector, Double>> targetStream = new TwoSeparableClassesDatasetGenerator(-1.0).labeled();

        targetStream.limit(3000).forEach(v -> {
            String vector = Arrays.stream(v.features().asArray())
                .mapToObj(x -> String.format("%.2f", x)).collect(Collectors.joining(","));
            System.out.print(String.format("[%d,%s],", v.label().intValue(), vector));
        });
    }
}
