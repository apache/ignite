/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.dataset.feature.extractor;

import java.util.Arrays;
import java.util.stream.IntStream;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
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
        DoubleArrayVectorizer<Integer> vectorizer = new DoubleArrayVectorizer<Integer>();
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
            Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>().labeled(i);
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
        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>()
            .labeled(Vectorizer.LabelCoordinate.FIRST);
        LabeledVector<Double> res = vectorizer.apply(1, features);
        assertEquals(res.features().size(), 2);
        assertArrayEquals(res.features().asArray(), new double[] {1., 2.}, 0.);
        assertEquals(0., res.label(), 0.);

        vectorizer = new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST);
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
        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>()
            .exclude(excludedIds)
            .labeled(Vectorizer.LabelCoordinate.FIRST);

        LabeledVector<Double> res = vectorizer.apply(1, features);
        assertEquals(res.features().size(), 2);
        assertArrayEquals(res.features().asArray(), new double[] {1., 99.}, 0.);
        assertEquals(0., res.label(), 0.);
    }
}
