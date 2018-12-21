package org.apache.ignite.ml.utils.generators;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.datastream.standard.TwoSeparableClassesDataStream;
import org.apache.ignite.ml.util.generators.primitives.variable.GaussRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.variable.UniformRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.vector.ParametricVectorGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorsFamily;
import org.junit.Test;

import static org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives.parallelogram;
import static org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives.gauss;
import static org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives.circle;
import static org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives.ring;

public class RandomVectorsGeneratorTest {
    @Test
    public void name() {
//        Stream<LabeledVector<Vector, Double>> targetStream = new GaussianLerpDataStream(2, 5, 1.0, -50, 50.0, System.currentTimeMillis()).labeled();
//        Stream<LabeledVector<Vector, Double>> targetStream = new ClassificationDataStream(2, 10, 0, 0, 2, 20.0, System.currentTimeMillis()).labeled();
//                Stream<LabeledVector<Vector, Double>> targetStream = new CirclesDataStream(5, 5, 5).labeled();
        Stream<LabeledVector<Vector, Double>> targetStream = new TwoSeparableClassesDataStream(-2.0, 10).labeled();
//        Stream<LabeledVector<Vector, Double>> targetStream = RegressionDataStream.twoDimensional(
//            (Math::sin, new GaussRandomProducer(0, 0.01)),
//            -10, 10
//        ).labeled();
//        Stream<LabeledVector<Vector, Double>> targetStream = new GaussianMixtureDataStream.Builder()
//            .add(VectorUtils.of(3.0, 3.0), 1.0)
//            .add(VectorUtils.of(-3.0, -3.0), 2.0)
//            .add(VectorUtils.of(-3.0, 3.0), 0.5)
//            .add(VectorUtils.of(3.0, -3.0), 1.5)
//            .build().labeled();
//

        GaussRandomProducer gauss = new GaussRandomProducer(0.0, 0.1);

        Long seed = System.currentTimeMillis();
        UniformRandomProducer rnd = new UniformRandomProducer(-10, 10);
        VectorGenerator noize = rnd.vectorize(2)
            .concat(new GaussRandomProducer(rnd.get(), Math.abs(rnd.get())).vectorize(3))
            .concat(new ParametricVectorGenerator(rnd, t -> t, t -> -t));

        targetStream = new VectorGeneratorsFamily.Builder()
            .add(ring(5, -Math.PI, 0).noisify(gauss).move(VectorUtils.of(-6.0, -2.5)))
            .add(ring(5, 0, Math.PI).noisify(gauss).move(VectorUtils.of(-7.0, 2.5)))
            .add(ring(5, 0, 2 * Math.PI).noisify(gauss).move(VectorUtils.of(7.0, 0.0)))
            .add(circle(1).move(VectorUtils.of(8.5, 1.5)))
            .add(parallelogram(VectorUtils.of(2, 2)).rotate(Math.PI / 4))
            .add(gauss(VectorUtils.of(0, 0), VectorUtils.of(0.2, 0.1)).move(VectorUtils.of(8.5, -1.5)))
            .add(gauss(VectorUtils.of(0, 0), VectorUtils.of(0.1, 0.2)).move(VectorUtils.of(5.5, -1.5)))
            .map(g -> g.concat(noize).duplicateRandomFeatures(3, seed).shuffle(seed))
            .build().asDataStream().labeled();

        targetStream.limit(3000).forEach(v -> {
            String vector = Arrays.stream(v.features().asArray())
                .mapToObj(x -> String.format("%.2f", x)).collect(Collectors.joining(","));
            System.out.print(String.format("[%.2f,%s],", v.label(), vector));
        });
    }
}
