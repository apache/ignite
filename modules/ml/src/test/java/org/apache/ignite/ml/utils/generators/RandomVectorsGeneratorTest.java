package org.apache.ignite.ml.utils.generators;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.datastream.standard.CirclesDataStream;
import org.apache.ignite.ml.util.generators.datastream.standard.GaussianMixtureDataStream;
import org.apache.ignite.ml.util.generators.datastream.standard.TwoSeparableClassesDataStream;
import org.junit.Test;

public class RandomVectorsGeneratorTest {
    @Test
    public void name() {
//                Stream<LabeledVector<Vector, Double>> targetStream = new CirclesDataStream(5, 5, 5).labeled();
//        Stream<LabeledVector<Vector, Double>> targetStream = new TwoSeparableClassesDataStream(2.0, 10).labeled();
        Stream<LabeledVector<Vector, Double>> targetStream = new GaussianMixtureDataStream.Builder()
            .add(VectorUtils.of(3.0, 3.0), 1.0)
            .add(VectorUtils.of(-3.0, -3.0), 2.0)
            .add(VectorUtils.of(-3.0, 3.0), 0.5)
            .add(VectorUtils.of(3.0, -3.0), 1.5)
            .build().labeled();
//
        targetStream.limit(3000).forEach(v -> {
            String vector = Arrays.stream(v.features().asArray())
                .mapToObj(x -> String.format("%.2f", x)).collect(Collectors.joining(","));
            System.out.print(String.format("[%d,%s],", v.label().intValue(), vector));
        });
    }
}
