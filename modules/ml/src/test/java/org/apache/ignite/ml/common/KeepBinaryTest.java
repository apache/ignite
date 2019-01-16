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

package org.apache.ignite.ml.common;

import java.util.Random;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.clustering.kmeans.KMeansTrainer;
import org.apache.ignite.ml.math.distances.ManhattanDistance;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.junit.Test;

/**
 * Test for IGNITE-10700.
 */
public class KeepBinaryTest {
    /*
     * Startup Ignite, populate cache and train some model.
     */
    @Test
    public void test() {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            IgniteCache<Integer, BinaryObject> dataCache = populateCache(ignite);

            IgniteBiFunction<Integer, BinaryObject, Vector> featureExtractor
                = (k, v) -> VectorUtils.of(new double[]{v.field("feat1")});
            IgniteBiFunction<Integer, BinaryObject, Double> lbExtractor = (k, v) -> (double) v.field("label");

            IgniteBiFunction<Integer, BinaryObject, Vector> normalizationPreprocessor = new NormalizationTrainer<Integer, BinaryObject>()
                .withP(1)
                .fit(
                    ignite,
                    dataCache,
                    featureExtractor
                );

            KMeansTrainer trainer = new KMeansTrainer().withDistance(new ManhattanDistance()).withSeed(7867L);

            KMeansModel kmdl = trainer.fit(ignite, dataCache, normalizationPreprocessor, lbExtractor);

        }
    }

    /**
     * Populate cache with binary objects.
     */
    private IgniteCache<Integer, BinaryObject> populateCache(Ignite ignite) {
        CacheConfiguration<Integer, BinaryObject> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName("TEST_" + UUID.randomUUID());

        IgniteCache<Integer, BinaryObject> cache = ignite.createCache(cacheConfiguration).withKeepBinary();

        BinaryObjectBuilder builder = ignite.binary().builder("testType");

        for (int i = 0; i < 1_000; i++) {
            BinaryObject value = builder.setField("label", (i < 500)? 0.0 : 1.0)
                .setField("feat1", new Random().nextDouble())
                .build();
            cache.put(i, value);
        }

        return cache;
    }
}
