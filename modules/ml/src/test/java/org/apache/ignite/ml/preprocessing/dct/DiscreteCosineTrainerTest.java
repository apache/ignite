package org.apache.ignite.ml.preprocessing.dct;

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link DiscreteCosineTrainer}.
 */
public class DiscreteCosineTrainerTest extends TrainerTest {
    /**
     * Tests {@code fit()} method.
     */
    @Test
    public void testFit() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(1, VectorUtils.of(2, 4, 1));
        data.put(2, VectorUtils.of(1, 8, 22));
        data.put(3, VectorUtils.of(4, 10, 100));
        data.put(4, VectorUtils.of(0, 22, 300));

        DatasetBuilder<Integer, Vector> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(0, 1, 2);

        DiscreteCosineTrainer<Integer, Vector> discreteCosineTrainer = new DiscreteCosineTrainer<>();

        DiscreteCosinePreprocessor<Integer, Vector> preprocessor = discreteCosineTrainer.fit(
                TestUtils.testEnvBuilder(),
                datasetBuilder,
                vectorizer
        );

        assertArrayEquals(new double[]{20, 0, -14}, preprocessor.apply(5, VectorUtils.of(1., 8., 1.)).features().asArray(), 1e-2);
    }
}