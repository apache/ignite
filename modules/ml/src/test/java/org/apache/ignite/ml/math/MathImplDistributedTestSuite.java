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

import org.apache.ignite.ml.math.impls.matrix.CacheMatrixTest;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedBlockMatrixTest;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrixTest;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseDistributedMatrixStorageTest;
import org.apache.ignite.ml.math.impls.storage.vector.SparseDistributedVectorStorageTest;
import org.apache.ignite.ml.math.impls.vector.CacheVectorTest;
import org.apache.ignite.ml.math.impls.vector.SparseBlockDistributedVectorTest;
import org.apache.ignite.ml.math.impls.vector.SparseDistributedVectorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all distributed tests located in org.apache.ignite.ml.math.impls.* package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    CacheVectorTest.class,
    CacheMatrixTest.class,
    SparseDistributedMatrixStorageTest.class,
    SparseDistributedMatrixTest.class,
    SparseDistributedBlockMatrixTest.class,
    SparseDistributedVectorStorageTest.class,
    SparseDistributedVectorTest.class,
    SparseBlockDistributedVectorTest.class
})
public class MathImplDistributedTestSuite {
    // No-op.
}
