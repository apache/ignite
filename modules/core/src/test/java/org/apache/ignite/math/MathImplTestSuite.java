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

import org.apache.ignite.math.impls.AbstractVectorTest;
import org.apache.ignite.math.impls.DenseLocalOffHeapVector;
import org.apache.ignite.math.impls.DenseLocalOffHeapVectorTest;
import org.apache.ignite.math.impls.DenseLocalOnHeapVectorConstructorTest;
import org.apache.ignite.math.impls.DenseLocalOnHeapVectorIterableTest;
import org.apache.ignite.math.impls.DenseLocalOnHeapVectorTest;
import org.apache.ignite.math.impls.storage.VectorArrayStorageTest;
import org.apache.ignite.math.impls.storage.VectorOffheapStorageTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all import tests located in org.apache.ignite.math.impls.* package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    AbstractVectorTest.class,
    DenseLocalOnHeapVectorConstructorTest.class,
    DenseLocalOnHeapVectorIterableTest.class,
    DenseLocalOnHeapVectorTest.class,
    DenseLocalOffHeapVectorTest.class,
    VectorArrayStorageTest.class,
    VectorOffheapStorageTest.class

})
public class MathImplTestSuite {
}
