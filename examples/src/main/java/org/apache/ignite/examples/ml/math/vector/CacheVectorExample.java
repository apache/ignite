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

package org.apache.ignite.examples.ml.math.vector;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.math.IdentityValueMapper;
import org.apache.ignite.ml.math.distributed.ValueMapper;
import org.apache.ignite.ml.math.distributed.VectorKeyMapper;
import org.apache.ignite.ml.math.impls.vector.CacheVector;

/**
 * This example shows how to use {@link CacheVector} API.
 * <p>
 * Basically CacheVector is a view over existing data in cache. So we have {@link VectorKeyMapper} and
 * {@link ValueMapper} for this purpose. A {@link VectorKeyMapper} allows us to map vector indices to cache keys.
 * And a {@link ValueMapper} allows us map cache object to vector elements - doubles.</p>
 * <p>
 * In this example we use simple flat mapping for keys and {@link IdentityValueMapper} for cache
 * objects because they are Doubles.</p>
 */
public class CacheVectorExample {
    /** */
    private static final String CACHE_NAME = CacheVectorExample.class.getSimpleName();

    /** */
    private static final int CARDINALITY = 10;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> CacheVector example started.");

            CacheConfiguration<Integer, Double> cfg = new CacheConfiguration<>();

            cfg.setName(CACHE_NAME);

            try (IgniteCache<Integer, Double> cache = ignite.getOrCreateCache(cfg)) {
                double[] testValues1 = {1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
                double[] testValues2 = {0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};

                ValueMapper valMapper = new IdentityValueMapper();

                // Map vector element index to cache keys.
                VectorKeyMapper<Integer> keyMapper1 = new VectorKeyMapper<Integer>() {
                    @Override public Integer apply(int i) {
                        return i;
                    }

                    @Override public boolean isValid(Integer integer) {
                        return integer >= 0 && CARDINALITY > integer;
                    }
                };

                // Map vector element index to cache keys with shift.
                VectorKeyMapper<Integer> keyMapper2 = new VectorKeyMapper<Integer>() {
                    @Override public Integer apply(int i) {
                        return i + CARDINALITY;
                    }

                    @Override public boolean isValid(Integer integer) {
                        return integer >= 0 && CARDINALITY > integer;
                    }
                };

                // Create two cache vectors over one cache.
                CacheVector cacheVector1 = new CacheVector(CARDINALITY, cache, keyMapper1, valMapper);
                System.out.println(">>> First cache vector created.");

                CacheVector cacheVector2 = new CacheVector(CARDINALITY, cache, keyMapper2, valMapper);
                System.out.println(">>> Second cache vector created.");

                cacheVector1.assign(testValues1);
                cacheVector2.assign(testValues2);

                // Dot product for orthogonal vectors is 0.0.
                assert cacheVector1.dot(cacheVector2) == 0.0;

                System.out.println(">>>");
                System.out.println(">>> Finished executing Ignite \"CacheVector\" example.");
                System.out.println(">>> Dot product is 0.0 for orthogonal vectors.");
                System.out.println(">>>");
            }
        }
    }
}
