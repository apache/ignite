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

package org.apache.ignite.examples.ml.math.matrix;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.math.IdentityValueMapper;
import org.apache.ignite.ml.math.MatrixKeyMapper;
import org.apache.ignite.ml.math.ValueMapper;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.impls.matrix.CacheMatrix;

/** */
public class CacheMatrixExample {
    /** */ private static final String CACHE_NAME = CacheMatrixExample.class.getSimpleName();
    /** */ private static final int ROWS = 3;
    /** */ private static final int COLS = 3;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> CacheMatrix example started.");

            CacheConfiguration<Integer, Double> cfg = new CacheConfiguration<>();

            cfg.setName(CACHE_NAME);

            try (IgniteCache<Integer, Double> cache = ignite.getOrCreateCache(cfg)) {
                double[][] testValues = {{1.0, 0.0, 0.0}, {1.0, 1.0, 0.0}, {1.0, 1.0, 1.0}};

                ValueMapper valMapper = new IdentityValueMapper();

                // Map matrix element indices to cache keys.
                MatrixKeyMapper<Integer> keyMapper = new MatrixKeyMapper<Integer>() {
                    @Override public Integer apply(int x, int y) {
                        return x * COLS + y;
                    }

                    @Override public boolean isValid(Integer integer) {
                        return integer >= 0 && integer < COLS * ROWS;
                    }
                };

                // Create cache matrix.
                CacheMatrix<Integer, Double> cacheMatrix = new CacheMatrix<>(ROWS, COLS, cache, keyMapper, valMapper);

                cacheMatrix.assign(testValues);

                // Find all positive elements.
                Integer nonZeroes = cacheMatrix.foldMap((o, aDouble) -> {
                    if (aDouble > 0)
                        return o + 1;
                    return o;
                }, Functions.IDENTITY, 0);

                assert nonZeroes.equals(6);

                System.out.println(">>>");
                System.out.println(">>> Finished executing Ignite \"CacheMatrix\" example.");
                System.out.println(">>> Lower triangular matrix 3x3 have only 6 positive elements.");
                System.out.println(">>>");
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(CACHE_NAME);
            }
        }
    }
}
