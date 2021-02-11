/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.preprocessing.encoding;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.ObjectArrayVectorizer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.exceptions.preprocessing.UnknownCategorialValueException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link EncoderTrainer}.
 */
public class EncoderTrainerTest extends TrainerTest {
    /** Tests {@code fit()} method. */
    @Test
    public void testFitOnStringCategorialFeatures() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(1, new DenseVector(new Serializable[] {1.0, "Monday", "September"}));
        data.put(2, new DenseVector(new Serializable[] {2.0, "Monday", "August"}));
        data.put(3, new DenseVector(new Serializable[] {3.0, "Monday", "August"}));
        data.put(4, new DenseVector(new Serializable[] {4.0, "Friday", "June"}));
        data.put(5, new DenseVector(new Serializable[] {5.0, "Friday", "June"}));
        data.put(6, new DenseVector(new Serializable[] {6.0, "Sunday", "August"}));

        final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>(1, 2).labeled(0);

        DatasetBuilder<Integer, Vector> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        EncoderTrainer<Integer, Vector> strEncoderTrainer = new EncoderTrainer<Integer, Vector>()
            .withEncoderType(EncoderType.STRING_ENCODER)
            .withEncodedFeature(0)
            .withEncodedFeature(1);

        EncoderPreprocessor<Integer, Vector> preprocessor = strEncoderTrainer.fit(
            TestUtils.testEnvBuilder(),
            datasetBuilder,
            vectorizer
        );

        assertArrayEquals(new double[] {0.0, 2.0}, preprocessor.apply(7, new DenseVector(new Serializable[] {7.0, "Monday", "September"})).features().asArray(), 1e-8);
    }

    /** Tests {@code fit()} method. */
    @Test
    public void testFitOnIntegerCategorialFeatures() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(1, VectorUtils.of(3.0, 0.0));
        data.put(2, VectorUtils.of(3.0, 12.0));
        data.put(3, VectorUtils.of(3.0, 12.0));
        data.put(4, VectorUtils.of(2.0, 45.0));
        data.put(5, VectorUtils.of(2.0, 45.0));
        data.put(6, VectorUtils.of(14.0, 12.0));

        final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(0, 1);

        DatasetBuilder<Integer, Vector> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        EncoderTrainer<Integer, Vector> strEncoderTrainer = new EncoderTrainer<Integer, Vector>()
            .withEncoderType(EncoderType.ONE_HOT_ENCODER)
            .withEncodedFeature(0)
            .withEncodedFeature(1);

        EncoderPreprocessor<Integer, Vector> preprocessor = strEncoderTrainer.fit(
            TestUtils.testEnvBuilder(),
            datasetBuilder,
            vectorizer
        );
        assertArrayEquals(new double[] {1.0, 0.0, 0.0, 0.0, 0.0, 1.0}, preprocessor.apply(7, VectorUtils.of(3.0, 0.0)).features().asArray(), 1e-8);
        assertArrayEquals(new double[] {0.0, 1.0, 0.0, 1.0, 0.0, 0.0}, preprocessor.apply(8, VectorUtils.of(2.0, 12.0)).features().asArray(), 1e-8);
    }

    /** Tests {@code fit()} method. */
    @Test
    public void testFitWithUnknownStringValueInTheGivenData() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(1, VectorUtils.of(3.0, 0.0));
        data.put(2, VectorUtils.of(3.0, 12.0));
        data.put(3, VectorUtils.of(3.0, 12.0));
        data.put(4, VectorUtils.of(2.0, 45.0));
        data.put(5, VectorUtils.of(2.0, 45.0));
        data.put(6, VectorUtils.of(14.0, 12.0));

        final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(0, 1);

        DatasetBuilder<Integer, Vector> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        EncoderTrainer<Integer, Vector> strEncoderTrainer = new EncoderTrainer<Integer, Vector>()
            .withEncoderType(EncoderType.STRING_ENCODER)
            .withEncodedFeature(0)
            .withEncodedFeature(1);

        EncoderPreprocessor<Integer, Vector> preprocessor = strEncoderTrainer.fit(
            TestUtils.testEnvBuilder(),
            datasetBuilder,
            vectorizer
        );

        try {
            preprocessor.apply(7, new DenseVector(new Serializable[] {"Monday", "September"})).features().asArray();
            fail("UnknownCategorialFeatureValue");
        }
        catch (UnknownCategorialValueException e) {
            return;
        }
        fail("UnknownCategorialFeatureValue");
    }

    /** Tests {@code fit()} method. */
    @Test
    public void testFitOnStringCategorialFeaturesWithReversedOrder() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(1, new DenseVector(new Serializable[] {"Monday", "September"}));
        data.put(2, new DenseVector(new Serializable[] {"Monday", "August"}));
        data.put(3, new DenseVector(new Serializable[] {"Monday", "August"}));
        data.put(4, new DenseVector(new Serializable[] {"Friday", "June"}));
        data.put(5, new DenseVector(new Serializable[] {"Friday", "June"}));
        data.put(6, new DenseVector(new Serializable[] {"Sunday", "August"}));

        final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(0, 1);

        DatasetBuilder<Integer, Vector> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        EncoderTrainer<Integer, Vector> strEncoderTrainer = new EncoderTrainer<Integer, Vector>()
            .withEncoderType(EncoderType.STRING_ENCODER)
            .withEncoderIndexingStrategy(EncoderSortingStrategy.FREQUENCY_ASC)
            .withEncodedFeature(0)
            .withEncodedFeature(1);

        EncoderPreprocessor<Integer, Vector> preprocessor = strEncoderTrainer.fit(
            TestUtils.testEnvBuilder(),
            datasetBuilder,
            vectorizer
        );

        assertArrayEquals(new double[] {2.0, 0.0}, preprocessor.apply(7, new DenseVector(new Serializable[] {"Monday", "September"})).features().asArray(), 1e-8);
    }

    /** Tests {@code fit()} method. */
    @Test
    public void testFitOnStringCategorialFeaturesWithFrequencyEncoding() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(1, new DenseVector(new Serializable[] {"Monday", "September"}));
        data.put(2, new DenseVector(new Serializable[] {"Monday", "August"}));
        data.put(3, new DenseVector(new Serializable[] {"Monday", "August"}));
        data.put(4, new DenseVector(new Serializable[] {"Friday", "June"}));
        data.put(5, new DenseVector(new Serializable[] {"Friday", "June"}));
        data.put(6, new DenseVector(new Serializable[] {"Sunday", "August"}));

        final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(0, 1);

        DatasetBuilder<Integer, Vector> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        EncoderTrainer<Integer, Vector> strEncoderTrainer = new EncoderTrainer<Integer, Vector>()
            .withEncoderType(EncoderType.FREQUENCY_ENCODER)
            .withEncodedFeature(0)
            .withEncodedFeature(1);

        EncoderPreprocessor<Integer, Vector> preprocessor = strEncoderTrainer.fit(
            TestUtils.testEnvBuilder(),
            datasetBuilder,
            vectorizer
        );

        assertArrayEquals(new double[] {0.5, 0.166}, preprocessor.apply(7, new DenseVector(new Serializable[] {"Monday", "September"})).features().asArray(), 0.1);
        assertArrayEquals(new double[] {0.33, 0.5}, preprocessor.apply(7, new DenseVector(new Serializable[] {"Friday", "August"})).features().asArray(), 0.1);
        assertArrayEquals(new double[] {0.166, 0.33}, preprocessor.apply(7, new DenseVector(new Serializable[] {"Sunday", "June"})).features().asArray(), 0.1);
    }

    /** Tests {@code fit()} method. */
    @Test
    public void testFitOnStringCategorialFeaturesAndLabels() {
        Map<Integer, Object[]> data = new HashMap<>();
        data.put(1, new Object[] {"1.0", 1.0, 2.0});
        data.put(2, new Object[] {"2.0", 2.0, 3.0});
        data.put(3, new Object[] {"1.0", 3.0, 1.0});
        data.put(4, new Object[] {"1.0", 2.0, 1.0});
        data.put(5, new Object[] {"1.0", 1.0, 1.0});
        data.put(6, new Object[] {"2.0", 1.0, 2.2});

        final Vectorizer<Integer, Object[], Integer, Object> vectorizer = new ObjectArrayVectorizer<Integer>(1, 2).labeled(0);

        DatasetBuilder<Integer, Object[]> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        EncoderTrainer<Integer, Object[]> labelEncoderTrainer = new EncoderTrainer<Integer, Object[]>()
            .withEncoderType(EncoderType.LABEL_ENCODER);

        EncoderPreprocessor<Integer, Object[]> preprocessor = labelEncoderTrainer.fit(
            TestUtils.testEnvBuilder(),
            datasetBuilder,
            vectorizer
        );

        assertEquals(0.0, (Double)preprocessor.apply(8, new Object[] {"1.0", 2.0, 3.0}).label(), 1e-8);
    }

    /** Tests {@code fit()} method. */
    @Test(expected = org.apache.ignite.ml.math.exceptions.preprocessing.IllegalFeatureTypeException.class)
    public void testFitWithExceptionOnMissedEncodedFeatureIndex() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(1, new DenseVector(new Serializable[] {1.0, "Monday", "September"}));
        data.put(2, new DenseVector(new Serializable[] {2.0, "Monday", "August"}));
        data.put(3, new DenseVector(new Serializable[] {3.0, "Monday", "August"}));
        data.put(4, new DenseVector(new Serializable[] {4.0, "Friday", "June"}));
        data.put(5, new DenseVector(new Serializable[] {5.0, "Friday", "June"}));
        data.put(6, new DenseVector(new Serializable[] {6.0, "Sunday", "August"}));

        final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>(1, 2).labeled(0);

        DatasetBuilder<Integer, Vector> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        EncoderTrainer<Integer, Vector> strEncoderTrainer = new EncoderTrainer<Integer, Vector>()
            .withEncoderType(EncoderType.STRING_ENCODER)
            .withEncodedFeature(0);

        EncoderPreprocessor<Integer, Vector> preprocessor = strEncoderTrainer.fit(
            TestUtils.testEnvBuilder(),
            datasetBuilder,
            vectorizer
        );

        assertArrayEquals(new double[] {0.0, 2.0}, preprocessor.apply(7, new DenseVector(new Serializable[] {7.0, "Monday", "September"})).features().asArray(), 1e-8);
    }
}
