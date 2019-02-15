/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.knn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.knn.classification.KNNClassificationTrainer;
import org.apache.ignite.ml.knn.classification.NNStrategy;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

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
        new KNNClassificationModel(null).apply(null);
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
            data,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(NNStrategy.SIMPLE);

        assertTrue(knnMdl.toString().length() > 0);
        assertTrue(knnMdl.toString(true).length() > 0);
        assertTrue(knnMdl.toString(false).length() > 0);

        Vector firstVector = new DenseVector(new double[] {2.0, 2.0});
        assertEquals(knnMdl.apply(firstVector), 1.0);
        Vector secondVector = new DenseVector(new double[] {-2.0, -2.0});
        assertEquals(knnMdl.apply(secondVector), 2.0);
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
            data,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        ).withK(1)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(NNStrategy.SIMPLE);

        Vector firstVector = new DenseVector(new double[] {2.0, 2.0});
        assertEquals(knnMdl.apply(firstVector), 1.0);
        Vector secondVector = new DenseVector(new double[] {-2.0, -2.0});
        assertEquals(knnMdl.apply(secondVector), 2.0);
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
            data,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(NNStrategy.SIMPLE);

        Vector vector = new DenseVector(new double[] {-1.01, -1.01});
        assertEquals(knnMdl.apply(vector), 2.0);
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
            data,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(NNStrategy.WEIGHTED);

        Vector vector = new DenseVector(new double[] {-1.01, -1.01});
        assertEquals(knnMdl.apply(vector), 1.0);
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
            data,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(NNStrategy.WEIGHTED);

        KNNClassificationModel updatedOnSameDataset = trainer.update(originalMdl,
            data, parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );

        KNNClassificationModel updatedOnEmptyDataset = trainer.update(originalMdl,
            new HashMap<Integer, double[]>(), parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );

        Vector vector = new DenseVector(new double[] {-1.01, -1.01});
        assertEquals(originalMdl.apply(vector), updatedOnSameDataset.apply(vector));
        assertEquals(originalMdl.apply(vector), updatedOnEmptyDataset.apply(vector));
    }
}
