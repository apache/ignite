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

import org.apache.ignite.math.impls.*;
import org.apache.ignite.math.impls.storage.matrix.MatrixArrayStorageTest;
import org.apache.ignite.math.impls.storage.matrix.MatrixNullStorageTest;
import org.apache.ignite.math.impls.storage.matrix.MatrixOffHeapStorageTest;
import org.apache.ignite.math.impls.storage.matrix.MatrixStorageImplementationTest;
import org.apache.ignite.math.impls.storage.vector.RandomAccessSparseVectorStorageTest;
import org.apache.ignite.math.impls.storage.vector.SequentialAccessSparseVectorStorageTest;
import org.apache.ignite.math.impls.storage.vector.VectorArrayStorageTest;
import org.apache.ignite.math.impls.storage.vector.VectorOffheapStorageTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all import tests located in org.apache.ignite.math.impls.* package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    AbstractVectorTest.class,
    DenseLocalOnHeapVectorConstructorTest.class,
    DenseLocalOffHeapVectorConstructorTest.class,
    RandomAccessSparseLocalOnHeapVectorConstructorTest.class,
    SequentialAccessSparseLocalOnHeapVectorConstructorTest.class,
    VectorIterableTest.class,
    VectorImplementationsTest.class,
    VectorAttributesTest.class,
    DenseLocalOffHeapVectorTest.class,
    VectorArrayStorageTest.class,
    VectorOffheapStorageTest.class,
    RandomAccessSparseVectorStorageTest.class,
    SequentialAccessSparseVectorStorageTest.class,
    VectorViewTest.class,
    MatrixOffHeapStorageTest.class,
    MatrixArrayStorageTest.class,
    DenseLocalOffHeapMatrixTest.class,
    MatrixNullStorageTest.class,
    VectorToMatrixTest.class,
    MatrixStorageImplementationTest.class
})
public class MathImplTestSuite {
}
