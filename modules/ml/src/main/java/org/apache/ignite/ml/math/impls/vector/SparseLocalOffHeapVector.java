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

package org.apache.ignite.ml.math.impls.vector;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.storage.vector.SparseLocalOffHeapVectorStorage;

/**
 * Implementation for {@link Vector} assuming sparse logic and local offheap JVM storage.
 * It is suitable for data sets where local, non-distributed execution is satisfactory and on-heap JVM storage
 * is not enough to keep the entire data set.
 * <p>See also: <a href="https://en.wikipedia.org/wiki/Sparse_array">Wikipedia article</a>.</p>
 */
public class SparseLocalOffHeapVector extends AbstractVector {
    /**
     * @param crd Vector cardinality.
     */
    public SparseLocalOffHeapVector(int crd) {
        setStorage(new SparseLocalOffHeapVectorStorage(crd));
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return new SparseLocalOffHeapVector(crd);
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return null;
    }
}
