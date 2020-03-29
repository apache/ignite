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

package org.apache.ignite.ml.clustering.gmm;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CovarianceMatricesAggregator}.
 */
public class CovarianceMatricesAggregatorTest {
    /** */
    @Test
    public void testAdd() {
        CovarianceMatricesAggregator agg = new CovarianceMatricesAggregator(VectorUtils.of(1., 0.));
        assertEquals(0, agg.rowCount());

        agg.add(VectorUtils.of(1., 0.), 100.);
        assertArrayEquals(VectorUtils.of(1., 0.).asArray(), agg.mean().asArray(), 1e-4);
        assertArrayEquals(
            agg.weightedSum().getStorage().data(),
            fromArray(2, 0., 0., 0., 0.).getStorage().data(),
            1e-4
        );
        assertEquals(1, agg.rowCount());

        agg.add(VectorUtils.of(0., 1.), 10.);
        assertArrayEquals(VectorUtils.of(1., 0.).asArray(), agg.mean().asArray(), 1e-4);
        assertArrayEquals(
            agg.weightedSum().getStorage().data(),
            fromArray(2, 10., -10., -10., 10.).getStorage().data(),
            1e-4
        );
        assertEquals(2, agg.rowCount());
    }

    /** */
    @Test
    public void testPlus() {
        Vector mean = VectorUtils.of(1, 0);

        CovarianceMatricesAggregator agg1 = new CovarianceMatricesAggregator(mean, identity(2), 1);
        CovarianceMatricesAggregator agg2 = new CovarianceMatricesAggregator(mean, identity(2).times(2), 3);
        CovarianceMatricesAggregator res = agg1.plus(agg2);

        assertArrayEquals(mean.asArray(), res.mean().asArray(), 1e-4);
        assertArrayEquals(identity(2).times(3).getStorage().data(), res.weightedSum().getStorage().data(), 1e-4);
        assertEquals(4, res.rowCount());
    }

    /** */
    @Test
    public void testReduce() {
        Vector mean1 = VectorUtils.of(1, 0);
        Vector mean2 = VectorUtils.of(0, 1);

        CovarianceMatricesAggregator agg11 = new CovarianceMatricesAggregator(mean1, identity(2), 1);
        CovarianceMatricesAggregator agg12 = new CovarianceMatricesAggregator(mean1, identity(2), 1);

        CovarianceMatricesAggregator agg21 = new CovarianceMatricesAggregator(mean2, identity(2), 2);
        CovarianceMatricesAggregator agg22 = new CovarianceMatricesAggregator(mean2, identity(2), 2);

        List<CovarianceMatricesAggregator> result = CovarianceMatricesAggregator.reduce(
            Arrays.asList(agg11, agg21),
            Arrays.asList(agg12, agg22)
        );

        assertEquals(2, result.size());
        CovarianceMatricesAggregator res1 = result.get(0);
        CovarianceMatricesAggregator res2 = result.get(1);

        assertArrayEquals(mean1.asArray(), res1.mean().asArray(), 1e-4);
        assertArrayEquals(identity(2).times(2).getStorage().data(), res1.weightedSum().getStorage().data(), 1e-4);
        assertEquals(2, res1.rowCount());

        assertArrayEquals(mean2.asArray(), res2.mean().asArray(), 1e-4);
        assertArrayEquals(identity(2).times(2).getStorage().data(), res2.weightedSum().getStorage().data(), 1e-4);
        assertEquals(4, res2.rowCount());
    }

    /** */
    @Test
    public void testMap() {
        List<LabeledVector<Double>> xs = Arrays.asList(
            new LabeledVector<>(VectorUtils.of(1, 0), 0.),
            new LabeledVector<>(VectorUtils.of(0, 1), 0.),
            new LabeledVector<>(VectorUtils.of(1, 1), 0.)
        );

        double[][] pcxi = new double[][] {
            new double[] {0.1, 0.2},
            new double[] {0.4, 0.3},
            new double[] {0.5, 0.6}
        };

        GmmPartitionData data = new GmmPartitionData(xs, pcxi);
        Vector mean1 = VectorUtils.of(1, 1);
        Vector mean2 = VectorUtils.of(0, 1);
        List<CovarianceMatricesAggregator> result = CovarianceMatricesAggregator.map(data, new Vector[] {mean1, mean2});

        assertEquals(pcxi[0].length, result.size());

        CovarianceMatricesAggregator res1 = result.get(0);
        assertArrayEquals(mean1.asArray(), res1.mean().asArray(), 1e-4);
        assertArrayEquals(
            res1.weightedSum().getStorage().data(),
            fromArray(2, 0.4, 0., 0., 0.1).getStorage().data(),
            1e-4
        );
        assertEquals(3, res1.rowCount());

        CovarianceMatricesAggregator res2 = result.get(1);
        assertArrayEquals(mean2.asArray(), res2.mean().asArray(), 1e-4);
        assertArrayEquals(
            res2.weightedSum().getStorage().data(),
            fromArray(2, 0.8, -0.2, -0.2, 0.2).getStorage().data(),
            1e-4
        );
        assertEquals(3, res2.rowCount());
    }

    /** */
    private Matrix identity(int n) {
        DenseMatrix matrix = new DenseMatrix(n, n);
        for (int i = 0; i < n; i++)
            matrix.set(i, i, 1.);
        return matrix;
    }

    /** */
    private Matrix fromArray(int n, double... values) {
        assertTrue(n == values.length / n);

        return new DenseMatrix(values, n);
    }
}
