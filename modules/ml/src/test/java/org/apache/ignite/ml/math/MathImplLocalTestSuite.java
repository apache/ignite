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

package org.apache.ignite.ml.math;

import org.apache.ignite.ml.math.distances.DistanceTest;
import org.apache.ignite.ml.math.isolve.lsqr.LSQROnHeapTest;
import org.apache.ignite.ml.math.primitives.matrix.DenseMatrixConstructorTest;
import org.apache.ignite.ml.math.primitives.matrix.MatrixArrayStorageTest;
import org.apache.ignite.ml.math.primitives.matrix.MatrixAttributeTest;
import org.apache.ignite.ml.math.primitives.matrix.MatrixStorageImplementationTest;
import org.apache.ignite.ml.math.primitives.matrix.MatrixViewConstructorTest;
import org.apache.ignite.ml.math.primitives.matrix.SparseMatrixConstructorTest;
import org.apache.ignite.ml.math.primitives.vector.AbstractVectorTest;
import org.apache.ignite.ml.math.primitives.vector.DelegatingVectorConstructorTest;
import org.apache.ignite.ml.math.primitives.vector.DenseVectorConstructorTest;
import org.apache.ignite.ml.math.primitives.vector.MatrixVectorViewTest;
import org.apache.ignite.ml.math.primitives.vector.SparseVectorConstructorTest;
import org.apache.ignite.ml.math.primitives.vector.VectorArrayStorageTest;
import org.apache.ignite.ml.math.primitives.vector.VectorAttributesTest;
import org.apache.ignite.ml.math.primitives.vector.VectorFoldMapTest;
import org.apache.ignite.ml.math.primitives.vector.VectorNormTest;
import org.apache.ignite.ml.math.primitives.vector.VectorToMatrixTest;
import org.apache.ignite.ml.math.primitives.vector.VectorViewTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all local tests located in org.apache.ignite.ml.math.impls.* package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Vector constructors tests.
    DenseVectorConstructorTest.class,
    SparseVectorConstructorTest.class,
    DelegatingVectorConstructorTest.class,
    // Various vectors tests.
    AbstractVectorTest.class,
    VectorViewTest.class,
    MatrixVectorViewTest.class,
    // Vector particular features tests.
    VectorAttributesTest.class,
    VectorToMatrixTest.class,
    VectorNormTest.class,
    VectorFoldMapTest.class,
    // Vector storage tests
    VectorArrayStorageTest.class,
    // Matrix storage tests.
    MatrixStorageImplementationTest.class,
    MatrixArrayStorageTest.class,
    // Matrix constructors tests.
    DenseMatrixConstructorTest.class,
    MatrixViewConstructorTest.class,
    SparseMatrixConstructorTest.class,
    // Matrix tests.
    MatrixAttributeTest.class,
    DistanceTest.class,
    LSQROnHeapTest.class
})
public class MathImplLocalTestSuite {
    // No-op.
}
