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
