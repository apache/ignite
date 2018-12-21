package org.apache.ignite.ml.utils.generators;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.primitives.scalar.GaussRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.scalar.UniformRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorsFamily;
import org.apache.ignite.ml.util.generators.standard.TwoSeparableClassesDataStream;
import org.junit.Test;
import org.mockito.verification.VerificationWithTimeout;

import static org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives.circle;
import static org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives.gauss;
import static org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives.parallelogram;
import static org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives.ring;

public class RandomVectorsGeneratorTest {
    @Test
    public void name() {
//        Stream<LabeledVector<Vector, Double>> targetStream = new GaussianLerpDataStream(2, 5, 1.0, -50, 50.0, System.currentTimeMillis()).labeled();
//        Stream<LabeledVector<Vector, Double>> targetStream = new ClassificationDataStream(2, 10, 0, 0, 2, 20.0, System.currentTimeMillis()).labeled();
//                Stream<LabeledVector<Vector, Double>> targetStream = new RingsDataStream(5, 5, 5).labeled();
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
        VectorGenerator noize = rnd.vectorize(1);

        VectorGenerator ringWithOutliners1 = new VectorGeneratorsFamily.Builder()
            .add(ring(5, -Math.PI, 0).noisify(gauss).move(VectorUtils.of(-6.0, -2.5)).concat(new UniformRandomProducer(-1, 1)), 250)
            .add(gauss(VectorUtils.of(0, 0), VectorUtils.of(.1, .1)).move(VectorUtils.of(0., -6)).concat(new UniformRandomProducer(0, 1)), 2)
            .add(gauss(VectorUtils.of(0, 0), VectorUtils.of(.1, .1)).move(VectorUtils.of(10., 7.5)).concat(new UniformRandomProducer(0, 1)), 2)
            .build();

        VectorGenerator ringWithOutliners2 = new VectorGeneratorsFamily.Builder()
            .add(ring(5, 0, Math.PI).noisify(gauss).move(VectorUtils.of(-7.0, 2.5)).concat(new UniformRandomProducer(-2, 0)), 250)
            .add(gauss(VectorUtils.of(0, 0), VectorUtils.of(1., 1.)).move(VectorUtils.of(-6., 0.)).concat(new UniformRandomProducer(0, 1)), 4)
            .add(gauss(VectorUtils.of(0, 0), VectorUtils.of(0.1, 0.1)).move(VectorUtils.of(7.5, -3.)).concat(new UniformRandomProducer(0, 1)), 4)
            .build();

        VectorGenerator parallelogramWithOutliners = new VectorGeneratorsFamily.Builder()
            .add(parallelogram(VectorUtils.of(1, 1)).rotate(Math.PI / 4).move(VectorUtils.of(5., 2.)), 100)
            .add(VectorGeneratorPrimitives.constant(VectorUtils.of(10., 15.)), 1)
            .build();

        targetStream = new VectorGeneratorsFamily.Builder()
            .add(ringWithOutliners1)
            .add(ringWithOutliners2)
            .add(ring(5, 0, 2 * Math.PI).noisify(gauss).move(VectorUtils.of(7.0, 0.0)).concat(new UniformRandomProducer(-0.5, 2)))
            .add(circle(1).move(VectorUtils.of(8.5, 1.5)).concat(new UniformRandomProducer(-1.5, 0.5)))
            .add(parallelogramWithOutliners)
            .add(gauss(VectorUtils.of(0, 0), VectorUtils.of(0.2, 0.1)).move(VectorUtils.of(8.5, -1.5)).concat(new UniformRandomProducer(-5, -4)))
            .add(gauss(VectorUtils.of(0, 0), VectorUtils.of(0.1, 0.2)).move(VectorUtils.of(5.5, -1.5)).concat(new UniformRandomProducer(4, 6)))
            .map(g -> g)
            .build().asDataStream().labeled();

        targetStream.limit(3000).forEach(v -> {
            String vector = Arrays.stream(v.features().asArray())
                .mapToObj(x -> String.format("%.2f", x)).collect(Collectors.joining(","));
            System.out.print(String.format("[%.2f,%s],", v.label(), vector));
        });
    }
}
