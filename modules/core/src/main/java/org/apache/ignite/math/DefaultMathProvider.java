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

import org.apache.ignite.math.impls.matrix.*;
import org.apache.ignite.math.impls.vector.*;
import java.io.*;
import java.util.*;

/**
 * Default built-in math provider.
 */
public class DefaultMathProvider implements MathProvider {
    /**
     * Creates default math provider.
     */
    public DefaultMathProvider() {
        // No-op.
    }

    @Override
    public Optional<Matrix> matrix(String flavor, Map<String, Object> args) {
        assert flavor != null;
        assert args != null;

        String flavorNorm = flavor.trim().toLowerCase();

        switch (flavorNorm) {
            case "random": return Optional.of(new RandomMatrix(args));
            case "function": return Optional.of(new FunctionMatrix(args));
            case "diagonal":return Optional.of(new DiagonalMatrix(args));
            case "cache.matrix": return Optional.of(new CacheMatrix(args));
            case "dense.local.onheap": return Optional.of(new DenseLocalOnHeapMatrix(args));
            case "dense.local.offheap": return Optional.of(new DenseLocalOffHeapMatrix(args));

            default:
                return Optional.empty();
        }
    }

    @Override
    public Optional<Vector> vector(String flavor, Map<String, Object> args) {
        assert flavor != null;
        assert args != null;

        String flavorNorm = flavor.trim().toLowerCase();

        switch (flavorNorm) {
            case "random": return Optional.of(new RandomVector(args));
            case "function": return Optional.of(new FunctionVector(args));
            case "constant": return Optional.of(new ConstantVector(args));
            case "cache.vector": return Optional.of(new CacheVector(args));
            case "single.value": return Optional.of(new SingleElementVector(args));
            case "delegate": return Optional.of(new DelegatingVector(args));
            case "dense.local.onheap": return Optional.of(new DenseLocalOnHeapVector(args));
            case "dense.local.offheap": return Optional.of(new DenseLocalOffHeapVector(args));
            case "sparse.local.onheap": return Optional.of(new SparseLocalVector(args));

            default:
                return Optional.empty();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }
}
