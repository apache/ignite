package org.apache.ignite.ml.dataset.feature.extractor;

import java.util.Arrays;
import java.util.stream.IntStream;
import org.apache.ignite.ml.dataset.feature.extractor.impl.ArraysVectorizer;
import org.apache.ignite.ml.structures.LabeledVector;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for vectorizer API.
 */
public class VectorizerTest {
    /** */
    @Test
    public void vectorizerShouldReturnAllFeaturesByDefault() {
        double[] features = {1., 2., 3.};
        ArraysVectorizer<Integer> vectorizer = new ArraysVectorizer<Integer>();
        LabeledVector<Double> res = vectorizer.apply(1, features);
        assertEquals(res.features().size(), 3);
        assertArrayEquals(res.features().asArray(), features, 0.);
        assertEquals(0., res.label(), 0.); //for doubles zero by default
    }

    /** */
    @Test
    public void vectorizerShouldSetLabelByCoordinate() {
        double[] features = {0., 1., 2.};
        for (int i = 0; i < features.length; i++) {
            Vectorizer<Integer, double[], Integer, Double> vectorizer = new ArraysVectorizer<Integer>().labeled(i);
            LabeledVector<Double> res = vectorizer.apply(1, features);
            assertEquals(res.features().size(), 2);

            final int filteredId = i;
            double[] expFeatures = Arrays.stream(features).filter(f -> Math.abs(f - features[filteredId]) > 0.01).toArray();
            assertArrayEquals(res.features().asArray(), expFeatures, 0.);
            assertEquals((double)i, res.label(), 0.);
        }
    }

    /** */
    @Test
    public void vectorizerShouldSetLabelByEnum() {
        double[] features = {0., 1., 2.};
        Vectorizer<Integer, double[], Integer, Double> vectorizer = new ArraysVectorizer<Integer>()
            .labeled(Vectorizer.LabelCoordinate.FIRST);
        LabeledVector<Double> res = vectorizer.apply(1, features);
        assertEquals(res.features().size(), 2);
        assertArrayEquals(res.features().asArray(), new double[] {1., 2.}, 0.);
        assertEquals(0., res.label(), 0.);

        vectorizer = new ArraysVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST);
        res = vectorizer.apply(1, features);
        assertEquals(res.features().size(), 2);
        assertArrayEquals(res.features().asArray(), new double[] {0., 1.}, 0.);
        assertEquals(2., res.label(), 0.);
    }

    /** */
    @Test
    public void vectorizerShouldBeAbleExcludeFeatures() {
        double[] features = IntStream.range(0, 100).mapToDouble(Double::valueOf).toArray();
        Integer[] excludedIds = IntStream.range(2, 99).boxed().toArray(Integer[]::new);
        Vectorizer<Integer, double[], Integer, Double> vectorizer = new ArraysVectorizer<Integer>()
            .exclude(excludedIds)
            .labeled(Vectorizer.LabelCoordinate.FIRST);

        LabeledVector<Double> res = vectorizer.apply(1, features);
        assertEquals(res.features().size(), 2);
        assertArrayEquals(res.features().asArray(), new double[] {1., 99.}, 0.);
        assertEquals(0., res.label(), 0.);
    }

    /** */
    @Test
    public void vectorizerCanBeMapped() {
        double[] features = new double[]{0., 1., 2.};
        Vectorizer<Integer, double[], Integer, double[]> vectorizer = new ArraysVectorizer<Integer>()
            .labeled(Vectorizer.LabelCoordinate.FIRST)
            .map(v -> v.features().labeled(new double[] {v.label()}));


        LabeledVector<double[]> res = vectorizer.apply(1, features);
        assertEquals(res.features().size(), 2);
        assertArrayEquals(res.features().asArray(), new double[] {1., 2.}, 0.);
        assertArrayEquals(new double[] {0.}, res.label(), 0.);
    }
}
