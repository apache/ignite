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

package org.apache.ignite.examples.ml;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.clustering.kmeans.KMeansTrainer;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.BinaryObjectVectorizer;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;

/**
 * Example of support model training with binary objects.
 */
public class TrainingWithBinaryObjectExample {
    /**
     * Run example.
     */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Model training over cached dataset with binary objects usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, BinaryObject> dataCache = null;
            try {
                dataCache = populateCache(ignite);

                // Create dataset builder with enabled support of keeping binary for upstream cache.
                CacheBasedDatasetBuilder<Integer, BinaryObject> datasetBuilder =
                    new CacheBasedDatasetBuilder<>(ignite, dataCache).withKeepBinary(true);

                Vectorizer<Integer, BinaryObject, String, Double> vectorizer =
                    new BinaryObjectVectorizer<Integer>("feature1").labeled("label");

                KMeansTrainer trainer = new KMeansTrainer();
                KMeansModel mdl = trainer.fit(datasetBuilder, vectorizer);

                System.out.println(">>> Model trained over binary objects. Model " + mdl);
            }
            finally {
                dataCache.destroy();
            }
        }
        finally {
            System.out.flush();
        }
    }

    /**
     * Populate cache with some binary objects.
     */
    private static IgniteCache<Integer, BinaryObject> populateCache(Ignite ignite) {
        CacheConfiguration<Integer, BinaryObject> cacheConfiguration = new CacheConfiguration<>();

        cacheConfiguration.setName("PERSONS");
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 2));

        IgniteCache<Integer, BinaryObject> cache = ignite.createCache(cacheConfiguration).withKeepBinary();

        BinaryObjectBuilder builder = ignite.binary().builder("testType");

        for (int i = 0; i < 100; i++) {
            if (i > 50)
                cache.put(i, builder.setField("feature1", 0.0).setField("label", 0.0).build());
            else
                cache.put(i, builder.setField("feature1", 1.0).setField("label", 1.0).build());
        }
        return cache;
    }
}
