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

package org.apache.ignite.ml.environment.deploy;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.pipeline.Pipeline;
import org.apache.ignite.ml.pipeline.PipelineMdl;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.binarization.BinarizationPreprocessor;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainer;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** */
public class MLDeployingTest extends GridCommonAbstractTest {
    /** */
    private static final String EXT_VECTORIZER = "org.apache.ignite.tests.p2p.ml.CustomVectorizer";

    /** */
    private static final String EXT_PREPROCESSOR_1 = "org.apache.ignite.tests.p2p.ml.CustomPreprocessor1";

    /** */
    private static final String EXT_PREPROCESSOR_2 = "org.apache.ignite.tests.p2p.ml.CustomPreprocessor2";

    /** */
    private static final int NUMBER_OF_COMPUTE_RETRIES = 3;

    /** */
    private Ignite ignite1;

    /** */
    private Ignite ignite2;

    /** */
    private Ignite ignite3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi()
            .setIpFinder(new TcpDiscoveryVmIpFinder()
                .setAddresses(Arrays.asList("127.0.0.1:47500..47509"))));

        cfg.setPeerClassLoadingEnabled(true);

        return cfg;
    }

    /** */
    @Before
    public void setUp() throws Exception {
        ignite1 = startGrid(1);
        ignite2 = startGrid(2);
    }

    /** */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testCustomVectorizer() throws Exception {
        testOnCache("testCustomVectorizer", cache -> {
            Vectorizer<Integer, Vector, Integer, Double> vectorizer = createVectorizer();
            fitAndTestModel(cache, vectorizer);
        });
    }

    /** */
    @Test
    public void testCustomPreprocessor() throws Exception {
        testOnCache("testCustomPreprocessor", cache -> {
            Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>();
            vectorizer = vectorizer.labeled(Vectorizer.LabelCoordinate.LAST);

            Preprocessor<Integer, Vector> customPreprocessor1 = createPreprocessor(vectorizer, EXT_PREPROCESSOR_1);
            Preprocessor<Integer, Vector> customPreprocessor2 = createPreprocessor(customPreprocessor1, EXT_PREPROCESSOR_2);

            Preprocessor<Integer, Vector> knownPreprocessor1 = new BinarizationPreprocessor<>(0.5, customPreprocessor1);
            Preprocessor<Integer, Vector> knownPreprocessor2 = new BinarizationPreprocessor<>(0.5, customPreprocessor2);

            Preprocessor<Integer, Vector> customPreprocessor3 = createPreprocessor(knownPreprocessor2, EXT_PREPROCESSOR_1);
            Preprocessor<Integer, Vector> customPreprocessor4 = createPreprocessor(customPreprocessor3, EXT_PREPROCESSOR_2);

            fitAndTestModel(cache, customPreprocessor1);
            fitAndTestModel(cache, customPreprocessor2);
            fitAndTestModel(cache, knownPreprocessor1);
            fitAndTestModel(cache, knownPreprocessor2);
            fitAndTestModel(cache, customPreprocessor3);
            fitAndTestModel(cache, customPreprocessor4);
        });
    }

    /** */
    @Test
    public void testPipeline() throws Exception {
        testOnCache("testPipeline", cache -> {
            Vectorizer<Integer, Vector, Integer, Double> vectorizer = createVectorizer();

            PipelineMdl<Integer, Vector> mdl = new Pipeline<Integer, Vector, Integer, Double>()
                .addVectorizer(vectorizer)
                .addPreprocessingTrainer(new ImputerTrainer<Integer, Vector>())
                .addPreprocessingTrainer(makePreprocessorTrainer(EXT_PREPROCESSOR_2))
                .addPreprocessingTrainer(new MinMaxScalerTrainer<Integer, Vector>())
                .addPreprocessingTrainer(makePreprocessorTrainer(EXT_PREPROCESSOR_1))
                .addPreprocessingTrainer(new NormalizationTrainer<Integer, Vector>()
                    .withP(1))
                .addTrainer(new DecisionTreeClassificationTrainer(5, 0))
                .fit(cache);

            assertEquals(0., mdl.predict(VectorUtils.of(0., 0.)), 1.);
        });
    }

    /** */
    private void testOnCache(String cacheName, TestCacheConsumer body) throws Exception {
        IgniteCache<Integer, Vector> cache = null;
        try {
            cache = prepareCache(ignite1, cacheName);
            body.accept(new CacheBasedDatasetBuilder<>(ignite1, cache)
                .withRetriesNumber(NUMBER_OF_COMPUTE_RETRIES));
        } finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /** */
    private void fitAndTestModel(CacheBasedDatasetBuilder<Integer, Vector> datasetBuilder,
        Preprocessor<Integer, Vector> preprocessor) {
        LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer();
        LogisticRegressionModel mdl = trainer.fit(datasetBuilder, preprocessor);

        // For this case any answer is valid.
        assertEquals(0., mdl.predict(VectorUtils.of(0., 0.)), 1.);
    }

    /** */
    private Vectorizer<Integer, Vector, Integer, Double> createVectorizer() throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException {
        ClassLoader ldr = getExternalClassLoader();
        Class<?> clazz = ldr.loadClass(EXT_VECTORIZER);

        Constructor ctor = clazz.getConstructor();
        Vectorizer<Integer, Vector, Integer, Double> vectorizer =
            (Vectorizer<Integer, Vector, Integer, Double>)ctor.newInstance();
        vectorizer = vectorizer.labeled(Vectorizer.LabelCoordinate.LAST);
        return vectorizer;
    }

    /** */
    private Preprocessor<Integer, Vector> createPreprocessor(Preprocessor<Integer, Vector> basePreprocessor,
        String clsName) throws Exception {
        ClassLoader ldr = getExternalClassLoader();
        Class<?> clazz = ldr.loadClass(clsName);

        Constructor ctor = clazz.getConstructor(Preprocessor.class);
        return (Preprocessor<Integer, Vector>)ctor.newInstance(basePreprocessor);
    }

    /** */
    @NotNull private PreprocessingTrainer makePreprocessorTrainer(String preprocessorClsName) throws Exception {
        return new PreprocessingTrainer() {
            @Override public Preprocessor fit(LearningEnvironmentBuilder envBuilder, DatasetBuilder datasetBuilder,
                Preprocessor basePreprocessor) {
                try {
                    return createPreprocessor(basePreprocessor, preprocessorClsName);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /** xor truth table. */
    private static final double[][] xor = {
        {0.0, 0.0, 0.0},
        {0.0, 1.0, 1.0},
        {1.0, 0.0, 1.0},
        {1.0, 1.0, 0.0}
    };

    /** */
    private IgniteCache<Integer, Vector> prepareCache(Ignite ignite, String cacheName) {
        IgniteCache<Integer, Vector> cache = ignite.getOrCreateCache(new CacheConfiguration<>(cacheName));

        for (int i = 0; i < xor.length; i++)
            cache.put(i, VectorUtils.of(xor[i]));

        return cache;
    }

    /** */
    @FunctionalInterface
    private static interface TestCacheConsumer {
        /**
         * @param datasetBuilder Dataset builder.
         */
        public void accept(CacheBasedDatasetBuilder<Integer, Vector> datasetBuilder) throws Exception;
    }
}
