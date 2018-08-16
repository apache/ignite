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

package org.apache.ignite.ml.common;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.clustering.kmeans.KMeansModelFormat;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.knn.classification.KNNModelFormat;
import org.apache.ignite.ml.knn.classification.NNStrategy;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.distances.HammingDistance;
import org.apache.ignite.ml.math.distances.ManhattanDistance;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.VectorizedViewMatrix;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.multiclass.LogRegressionMultiClassModel;
import org.apache.ignite.ml.structures.Dataset;
import org.apache.ignite.ml.structures.DatasetRow;
import org.apache.ignite.ml.structures.FeatureMetadata;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.svm.SVMLinearBinaryClassificationModel;
import org.apache.ignite.ml.svm.SVMLinearMultiClassClassificationModel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests for equals and hashCode methods in classes that provide own implementations of these.
 */
public class CollectionsTest {
    /** */
    @Test
    @SuppressWarnings("unchecked")
    public void test() {
        test(new VectorizedViewMatrix(new DenseMatrix(2, 2), 1, 1, 1, 1),
            new VectorizedViewMatrix(new DenseMatrix(3, 2), 2, 1, 1, 1));

        specialTest(new ManhattanDistance(), new ManhattanDistance());

        specialTest(new HammingDistance(), new HammingDistance());

        specialTest(new EuclideanDistance(), new EuclideanDistance());

        FeatureMetadata data = new FeatureMetadata("name2");
        data.setName("name1");
        test(data, new FeatureMetadata("name2"));

        test(new DatasetRow<>(new DenseVector()), new DatasetRow<>(new DenseVector(1)));

        test(new LabeledVector<>(new DenseVector(), null), new LabeledVector<>(new DenseVector(1), null));

        test(new Dataset<DatasetRow<Vector>>(new DatasetRow[] {}, new FeatureMetadata[] {}),
            new Dataset<DatasetRow<Vector>>(new DatasetRow[] {new DatasetRow()},
                new FeatureMetadata[] {new FeatureMetadata()}));

        test(new LogisticRegressionModel(new DenseVector(), 1.0),
            new LogisticRegressionModel(new DenseVector(), 0.5));

        test(new KMeansModelFormat(new Vector[] {}, new ManhattanDistance()),
            new KMeansModelFormat(new Vector[] {}, new HammingDistance()));

        test(new KMeansModel(new Vector[] {}, new ManhattanDistance()),
            new KMeansModel(new Vector[] {}, new HammingDistance()));

        test(new KNNModelFormat(1, new ManhattanDistance(), NNStrategy.SIMPLE),
            new KNNModelFormat(2, new ManhattanDistance(), NNStrategy.SIMPLE));

        test(new KNNClassificationModel(null).withK(1), new KNNClassificationModel(null).withK(2));

        LogRegressionMultiClassModel mdl = new LogRegressionMultiClassModel();
        mdl.add(1, new LogisticRegressionModel(new DenseVector(), 1.0));
        test(mdl, new LogRegressionMultiClassModel());

        test(new LinearRegressionModel(null, 1.0), new LinearRegressionModel(null, 0.5));

        SVMLinearMultiClassClassificationModel mdl1 = new SVMLinearMultiClassClassificationModel();
        mdl1.add(1, new SVMLinearBinaryClassificationModel(new DenseVector(), 1.0));
        test(mdl1, new SVMLinearMultiClassClassificationModel());

        test(new SVMLinearBinaryClassificationModel(null, 1.0), new SVMLinearBinaryClassificationModel(null, 0.5));
    }

    /** Test classes that have all instances equal (eg, metrics). */
    private <T> void specialTest(T o1, T o2) {
        assertEquals(o1, o2);

        test(o1, new Object());
    }

    /** */
    private <T> void test(T o1, T o2) {
        assertNotEquals(o1, null);
        assertNotEquals(o2, null);

        assertEquals(o1, o1);
        assertEquals(o2, o2);

        assertNotEquals(o1, o2);

        Set<T> set = new HashSet<>();
        set.add(o1);
        set.add(o1);
        assertEquals(1, set.size());

        set.add(o2);
        set.add(o2);
        assertEquals(2, set.size());

        set.remove(o1);
        assertEquals(1, set.size());

        set.remove(o2);
        assertEquals(0, set.size());
    }
}
