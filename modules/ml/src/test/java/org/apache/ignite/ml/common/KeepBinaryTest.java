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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.clustering.kmeans.KMeansTrainer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.BinaryObjectVectorizer;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for IGNITE-10700.
 */
public class KeepBinaryTest extends GridCommonAbstractTest {
    /** Number of nodes in grid. */
    private static final int NODE_COUNT = 2;

    /** Number of samples. */
    public static final int NUMBER_OF_SAMPLES = 1000;

    /** Half of samples. */
    public static final int HALF = NUMBER_OF_SAMPLES / 2;

    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /**
     * Startup Ignite, populate cache and train some model.
     */
    @Test
    public void test() {
        IgniteCache<Integer, BinaryObject> dataCache = populateCache(ignite);

        KMeansTrainer trainer = new KMeansTrainer();

        CacheBasedDatasetBuilder<Integer, BinaryObject> datasetBuilder =
            new CacheBasedDatasetBuilder<>(ignite, dataCache).withKeepBinary(true);

        KMeansModel mdl = trainer.fit(datasetBuilder, new BinaryObjectVectorizer<Integer>("feature1").labeled("label"));

        Integer zeroCentre = mdl.predict(VectorUtils.num2Vec(0.0));

        assertTrue(mdl.getCenters()[zeroCentre].get(0) == 0);
    }

    /**
     * Populate cache with binary objects.
     */
    private IgniteCache<Integer, BinaryObject> populateCache(Ignite ignite) {
        CacheConfiguration<Integer, BinaryObject> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName("TEST_" + UUID.randomUUID());

        IgniteCache<Integer, BinaryObject> cache = ignite.createCache(cacheConfiguration).withKeepBinary();

        BinaryObjectBuilder builder = ignite.binary().builder("testType");

        for (int i = 0; i < NUMBER_OF_SAMPLES; i++) {
            if (i < HALF)
                cache.put(i, builder.setField("feature1", 0.0).setField("label", 0.0).build());
            else
                cache.put(i, builder.setField("feature1", 10.0).setField("label", 1.0).build());
        }

        return cache;
    }
}
