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

package org.apache.ignite.math;

import org.apache.ignite.math.decompositions.LUDecompositionTest;
import org.apache.ignite.math.impls.matrix.DiagonalMatrixTest;
import org.apache.ignite.math.impls.matrix.MatrixImplementationsTest;
import org.apache.ignite.math.impls.storage.matrix.MatrixArrayStorageTest;
import org.apache.ignite.math.impls.storage.matrix.MatrixOffHeapStorageTest;
import org.apache.ignite.math.impls.storage.matrix.MatrixStorageImplementationTest;
import org.apache.ignite.math.impls.storage.vector.RandomAccessSparseVectorStorageTest;
import org.apache.ignite.math.impls.storage.vector.VectorArrayStorageTest;
import org.apache.ignite.math.impls.storage.vector.VectorOffheapStorageTest;
import org.apache.ignite.math.impls.vector.*;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all local tests located in org.apache.ignite.math.impls.* package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Vector constructors tests.
    DenseLocalOnHeapVectorConstructorTest.class,
    DenseLocalOffHeapVectorConstructorTest.class,
    SparseLocalVectorConstructorTest.class,
    RandomVectorConstructorTest.class,
    ConstantVectorConstructorTest.class,
    FunctionVectorConstructorTest.class,
    SingleElementVectorConstructorTest.class,
    PivotedVectorViewConstructorTest.class,
    SingleElementVectorViewConstructorTest.class,
    DelegatingVectorConstructorTest.class,
    // Various vectors tests.
    AbstractVectorTest.class,
    VectorImplementationsTest.class,
    VectorViewTest.class,
    MatrixVectorViewTest.class,
    // Vector particular features tests.
    VectorIterableTest.class,
    VectorAttributesTest.class,
    VectorToMatrixTest.class,
    VectorNormTest.class,
    // Vector storage tests
    VectorArrayStorageTest.class,
    VectorOffheapStorageTest.class,
    RandomAccessSparseVectorStorageTest.class,
    // Matrix storage tests.
    MatrixStorageImplementationTest.class,
    MatrixOffHeapStorageTest.class,
    MatrixArrayStorageTest.class,
    // Matrix tests.
    MatrixImplementationsTest.class,
    DiagonalMatrixTest.class,
    // Decomposes
    LUDecompositionTest.class
})
public class MathImplLocalTestSuite {
    // No-op.
}
