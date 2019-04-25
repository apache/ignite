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

package org.apache.ignite.ml.knn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.knn.classification.KNNClassificationTrainer;
import org.apache.ignite.ml.knn.classification.NNStrategy;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests behaviour of KNNClassification. */
@RunWith(Parameterized.class)
public class KNNClassificationTest {
    /** Number of parts to be tested. */
    private static final int[] partsToBeTested = new int[] {1, 2, 3, 4, 5, 7, 100};

    /** Number of partitions. */
    @Parameterized.Parameter
    public int parts;

    /** Parameters. */
    @Parameterized.Parameters(name = "Data divided on {0} partitions, training with batch size {1}")
    public static Iterable<Integer[]> data() {
        List<Integer[]> res = new ArrayList<>();

        for (int part : partsToBeTested)
            res.add(new Integer[] {part});

        return res;
    }

    /** */
    @Test(expected = IllegalStateException.class)
    public void testNullDataset() {
        new KNNClassificationModel(null).predict(null);
    }

    /** */
    @Test
    public void testBinaryClassification() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {1.0, 1.0, 1.0});
        data.put(1, new double[] {1.0, 2.0, 1.0});
        data.put(2, new double[] {2.0, 1.0, 1.0});
        data.put(3, new double[] {-1.0, -1.0, 2.0});
        data.put(4, new double[] {-1.0, -2.0, 2.0});
        data.put(5, new double[] {-2.0, -1.0, 2.0});

        KNNClassificationTrainer trainer = new KNNClassificationTrainer();

        NNClassificationModel knnMdl = trainer.fit(
            data, parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(NNStrategy.SIMPLE);

        assertTrue(!knnMdl.toString().isEmpty());
        assertTrue(!knnMdl.toString(true).isEmpty());
        assertTrue(!knnMdl.toString(false).isEmpty());

        Vector firstVector = new DenseVector(new double[] {2.0, 2.0});
        assertEquals(1.0, knnMdl.predict(firstVector), 0);
        Vector secondVector = new DenseVector(new double[] {-2.0, -2.0});
        assertEquals(2.0, knnMdl.predict(secondVector), 0);
    }

    /** */
    @Test
    public void testBinaryClassificationWithSmallestK() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {1.0, 1.0, 1.0});
        data.put(1, new double[] {1.0, 2.0, 1.0});
        data.put(2, new double[] {2.0, 1.0, 1.0});
        data.put(3, new double[] {-1.0, -1.0, 2.0});
        data.put(4, new double[] {-1.0, -2.0, 2.0});
        data.put(5, new double[] {-2.0, -1.0, 2.0});

        KNNClassificationTrainer trainer = new KNNClassificationTrainer();

        NNClassificationModel knnMdl = trainer.fit(
            data, parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        ).withK(1)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(NNStrategy.SIMPLE);

        Vector firstVector = new DenseVector(new double[] {2.0, 2.0});
        assertEquals(1.0, knnMdl.predict(firstVector), 0);
        Vector secondVector = new DenseVector(new double[] {-2.0, -2.0});
        assertEquals(2.0, knnMdl.predict(secondVector), 0);
    }

    /** */
    @Test
    public void testBinaryClassificationFarPointsWithSimpleStrategy() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {10.0, 10.0, 1.0});
        data.put(1, new double[] {10.0, 20.0, 1.0});
        data.put(2, new double[] {-1, -1, 1.0});
        data.put(3, new double[] {-2, -2, 2.0});
        data.put(4, new double[] {-1.0, -2.0, 2.0});
        data.put(5, new double[] {-2.0, -1.0, 2.0});

        KNNClassificationTrainer trainer = new KNNClassificationTrainer();

        NNClassificationModel knnMdl = trainer.fit(
            data, parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(NNStrategy.SIMPLE);

        Vector vector = new DenseVector(new double[] {-1.01, -1.01});
        assertEquals(2.0, knnMdl.predict(vector), 0);
    }

    /** */
    @Test
    public void testBinaryClassificationFarPointsWithWeightedStrategy() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {10.0, 10.0, 1.0});
        data.put(1, new double[] {10.0, 20.0, 1.0});
        data.put(2, new double[] {-1, -1, 1.0});
        data.put(3, new double[] {-2, -2, 2.0});
        data.put(4, new double[] {-1.0, -2.0, 2.0});
        data.put(5, new double[] {-2.0, -1.0, 2.0});

        KNNClassificationTrainer trainer = new KNNClassificationTrainer();

        NNClassificationModel knnMdl = trainer.fit(
            data, parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(NNStrategy.WEIGHTED);

        Vector vector = new DenseVector(new double[] {-1.01, -1.01});
        assertEquals(1.0, knnMdl.predict(vector), 0);
    }

    /** */
    @Test
    public void testUpdate() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {10.0, 10.0, 1.0});
        data.put(1, new double[] {10.0, 20.0, 1.0});
        data.put(2, new double[] {-1, -1, 1.0});
        data.put(3, new double[] {-2, -2, 2.0});
        data.put(4, new double[] {-1.0, -2.0, 2.0});
        data.put(5, new double[] {-2.0, -1.0, 2.0});

        KNNClassificationTrainer trainer = new KNNClassificationTrainer();

        KNNClassificationModel originalMdl = (KNNClassificationModel)trainer.fit(
            data, parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(NNStrategy.WEIGHTED);

        KNNClassificationModel updatedOnSameDataset = trainer.update(originalMdl,
            data, parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );

        KNNClassificationModel updatedOnEmptyDataset = trainer.update(originalMdl,
            new HashMap<Integer, double[]>(), parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );

        Vector vector = new DenseVector(new double[] {-1.01, -1.01});
        assertEquals(originalMdl.predict(vector), updatedOnSameDataset.predict(vector));
        assertEquals(originalMdl.predict(vector), updatedOnEmptyDataset.predict(vector));
    }
}
