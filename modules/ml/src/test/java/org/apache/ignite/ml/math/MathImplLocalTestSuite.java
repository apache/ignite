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
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOffHeapMatrixConstructorTest;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrixConstructorTest;
import org.apache.ignite.ml.math.impls.matrix.MatrixAttributeTest;
import org.apache.ignite.ml.math.impls.matrix.MatrixImplementationsTest;
import org.apache.ignite.ml.math.impls.matrix.MatrixViewConstructorTest;
import org.apache.ignite.ml.math.impls.matrix.SparseLocalOnHeapMatrixConstructorTest;
import org.apache.ignite.ml.math.impls.storage.matrix.MatrixArrayStorageTest;
import org.apache.ignite.ml.math.impls.storage.matrix.MatrixOffHeapStorageTest;
import org.apache.ignite.ml.math.impls.storage.matrix.MatrixStorageImplementationTest;
import org.apache.ignite.ml.math.impls.storage.vector.RandomAccessSparseVectorStorageTest;
import org.apache.ignite.ml.math.impls.storage.vector.SparseLocalOffHeapVectorStorageTest;
import org.apache.ignite.ml.math.impls.storage.vector.VectorArrayStorageTest;
import org.apache.ignite.ml.math.impls.storage.vector.VectorOffheapStorageTest;
import org.apache.ignite.ml.math.impls.vector.AbstractVectorTest;
import org.apache.ignite.ml.math.impls.vector.DelegatingVectorConstructorTest;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOffHeapVectorConstructorTest;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVectorConstructorTest;
import org.apache.ignite.ml.math.impls.vector.MatrixVectorViewTest;
import org.apache.ignite.ml.math.impls.vector.SparseLocalVectorConstructorTest;
import org.apache.ignite.ml.math.impls.vector.VectorAttributesTest;
import org.apache.ignite.ml.math.impls.vector.VectorFoldMapTest;
import org.apache.ignite.ml.math.impls.vector.VectorNormTest;
import org.apache.ignite.ml.math.impls.vector.VectorToMatrixTest;
import org.apache.ignite.ml.math.impls.vector.VectorViewTest;
import org.apache.ignite.ml.math.isolve.lsqr.LSQROnHeapTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all local tests located in org.apache.ignite.ml.math.impls.* package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Vector constructors tests.
    DenseLocalOnHeapVectorConstructorTest.class,
    DenseLocalOffHeapVectorConstructorTest.class,
    SparseLocalVectorConstructorTest.class,
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
    VectorOffheapStorageTest.class,
    RandomAccessSparseVectorStorageTest.class,
    SparseLocalOffHeapVectorStorageTest.class,
    // Matrix storage tests.
    MatrixStorageImplementationTest.class,
    MatrixOffHeapStorageTest.class,
    MatrixArrayStorageTest.class,
    // Matrix constructors tests.
    DenseLocalOnHeapMatrixConstructorTest.class,
    DenseLocalOffHeapMatrixConstructorTest.class,
    MatrixViewConstructorTest.class,
    SparseLocalOnHeapMatrixConstructorTest.class,
    // Matrix tests.
    MatrixImplementationsTest.class,
    MatrixAttributeTest.class,
    DistanceTest.class,
    LSQROnHeapTest.class
})
public class MathImplLocalTestSuite {
    // No-op.
}
