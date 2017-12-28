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

package org.apache.ignite.ml.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.ml.examples.CacheMatrixExampleSelfTest;
import org.apache.ignite.ml.examples.CacheVectorExampleSelfTest;
import org.apache.ignite.ml.examples.CholeskyDecompositionExampleSelfTest;
import org.apache.ignite.ml.examples.DistributedRegressionExampleSelfTest;
import org.apache.ignite.ml.examples.DistributedRegressionModelExampleSelfTest;
import org.apache.ignite.ml.examples.EigenDecompositionExampleSelfTest;
import org.apache.ignite.ml.examples.FuzzyCMeansExampleSelfTest;
import org.apache.ignite.ml.examples.KMeansDistributedClustererExampleSelfTest;
import org.apache.ignite.ml.examples.KMeansLocalClustererExampleSelfTest;
import org.apache.ignite.ml.examples.KNNClassificationExampleSelfTest;
import org.apache.ignite.ml.examples.KNNRegressionExampleSelfTest;
import org.apache.ignite.ml.examples.LUDecompositionExampleSelfTest;
import org.apache.ignite.ml.examples.MNISTExampleSelfTest;
import org.apache.ignite.ml.examples.MatrixCustomStorageExampleSelfTest;
import org.apache.ignite.ml.examples.MatrixExampleSelfTest;
import org.apache.ignite.ml.examples.OffHeapMatrixExampleSelfTest;
import org.apache.ignite.ml.examples.OffHeapVectorExampleSelfTest;
import org.apache.ignite.ml.examples.QRDecompositionExampleSelfTest;
import org.apache.ignite.ml.examples.SingularValueDecompositionExampleSelfTest;
import org.apache.ignite.ml.examples.SparseDistributedMatrixExampleSelfTest;
import org.apache.ignite.ml.examples.SparseMatrixExampleSelfTest;
import org.apache.ignite.ml.examples.SparseVectorExampleSelfTest;
import org.apache.ignite.ml.examples.TracerExampleSelfTest;
import org.apache.ignite.ml.examples.VectorCustomStorageExampleSelfTest;
import org.apache.ignite.ml.examples.VectorExampleSelfTest;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Examples test suite.
 * <p>
 * Contains only Spring ignite examples tests.
 */
public class IgniteExamplesMLTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteExamplesMLTestSuite.class));

        TestSuite suite = new TestSuite("Ignite ML Examples Test Suite");

        suite.addTest(new TestSuite(CacheMatrixExampleSelfTest.class));
        suite.addTest(new TestSuite(CacheVectorExampleSelfTest.class));
        suite.addTest(new TestSuite(CholeskyDecompositionExampleSelfTest.class));
        suite.addTest(new TestSuite(DistributedRegressionExampleSelfTest.class));
        suite.addTest(new TestSuite(DistributedRegressionModelExampleSelfTest.class));
        suite.addTest(new TestSuite(EigenDecompositionExampleSelfTest.class));
        suite.addTest(new TestSuite(FuzzyCMeansExampleSelfTest.class));
        suite.addTest(new TestSuite(KMeansDistributedClustererExampleSelfTest.class));
        suite.addTest(new TestSuite(KMeansLocalClustererExampleSelfTest.class));
        suite.addTest(new TestSuite(KNNClassificationExampleSelfTest.class));
        suite.addTest(new TestSuite(KNNRegressionExampleSelfTest.class));
        suite.addTest(new TestSuite(LUDecompositionExampleSelfTest.class));
        suite.addTest(new TestSuite(MatrixCustomStorageExampleSelfTest.class));
        suite.addTest(new TestSuite(MatrixExampleSelfTest.class));
        suite.addTest(new TestSuite(MNISTExampleSelfTest.class));
        suite.addTest(new TestSuite(OffHeapMatrixExampleSelfTest.class));
        suite.addTest(new TestSuite(OffHeapVectorExampleSelfTest.class));
        suite.addTest(new TestSuite(QRDecompositionExampleSelfTest.class));
        suite.addTest(new TestSuite(SingularValueDecompositionExampleSelfTest.class));
        suite.addTest(new TestSuite(SparseDistributedMatrixExampleSelfTest.class));
        suite.addTest(new TestSuite(SparseMatrixExampleSelfTest.class));
        suite.addTest(new TestSuite(SparseVectorExampleSelfTest.class));
        suite.addTest(new TestSuite(TracerExampleSelfTest.class));
        suite.addTest(new TestSuite(VectorCustomStorageExampleSelfTest.class));
        suite.addTest(new TestSuite(VectorExampleSelfTest.class));

        return suite;
    }
}
