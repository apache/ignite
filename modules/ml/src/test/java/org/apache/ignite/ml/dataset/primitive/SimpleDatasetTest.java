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

package org.apache.ignite.ml.dataset.primitive;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.dataset.DatasetFactory;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link SimpleDataset}.
 */
public class SimpleDatasetTest {
    /** Basic test for SimpleDataset features. IMPL NOTE derived from LocalDatasetExample. */
    @Test
    public void basicTest() throws Exception {
        Map<Integer, DataPoint> dataPoints = new HashMap<Integer, DataPoint>() {{
            put(1, new DataPoint(42, 10000));
            put(2, new DataPoint(32, 64000));
            put(3, new DataPoint(53, 120000));
            put(4, new DataPoint(24, 70000));
        }};

        // Creates a local simple dataset containing features and providing standard dataset API.
        try (SimpleDataset<?> dataset = DatasetFactory.createSimpleDataset(
            dataPoints,
            2,
            (k, v) -> VectorUtils.of(v.getAge(), v.getSalary())
        )) {
            assertArrayEquals("Mean values.", new double[] {37.75, 66000.0}, dataset.mean(), 0);

            assertArrayEquals("Standard deviation values.",
                new double[] {10.871407452579449, 38961.519477556314}, dataset.std(), 0);

            double[][] covExp = new double[][] {
                new double[] {118.1875, 135500.0},
                new double[] {135500.0, 1.518E9}
            };
            double[][] cov = dataset.cov();
            int rowCov = 0;
            for (double[] row : cov)
                assertArrayEquals("Covariance matrix row " + rowCov,
                    covExp[rowCov++], row, 0);


            double[][] corrExp = new double[][] {
                new double[] {1.0000000000000002, 0.31990250167874007},
                new double[] {0.31990250167874007, 1.0}
            };
            double[][] corr = dataset.corr();
            int rowCorr = 0;
            for (double[] row : corr)
                assertArrayEquals("Correlation matrix row " + rowCorr,
                    corrExp[rowCorr++], row, 0);
        }
    }

    /** */
    private static class DataPoint {
        /** Age. */
        private final double age;

        /** Salary. */
        private final double salary;

        /**
         * Constructs a new instance of person.
         *
         * @param age Age.
         * @param salary Salary.
         */
        DataPoint(double age, double salary) {
            this.age = age;
            this.salary = salary;
        }

        /** */
        double getAge() {
            return age;
        }

        /** */
        double getSalary() {
            return salary;
        }
    }
}
