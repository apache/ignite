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

package org.apache.ignite.math.impls;

import org.apache.ignite.*;
import org.apache.ignite.math.*;

/**
 * Sparse distributes matrix view over the existing arbitrary cache.
 */
public class SparseDistributedCacheMatrix extends AbstractMatrix {
    /**
     *
     */
    public SparseDistributedCacheMatrix() {
        // No-op.
    }
    
    /**
     * Creates new matrix over existing cache.
     * 
     * @param rows
     * @param cols
     * @param cache
     * @param keyFunc
     * @param valMapper
     * @param <K>
     * @param <V>
     */
    public <K, V> SparseDistributedCacheMatrix(
        int rows,
        int cols,
        IgniteCache<K, V> cache,
        IntIntToKFunction<K> keyFunc,
        DoubleMapper<V> valMapper) {
        
    }

    @Override
    public Matrix copy() {
        return null; // TODO
    }

    @Override
    public Matrix like(int rows, int cols) {
        return null; // TODO
    }

    @Override
    public Vector likeVector(int crd) {
        return null; // TODO
    }
}
