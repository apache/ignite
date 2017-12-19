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

package org.apache.ignite.ml.trees.performance;

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.estimators.Estimators;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distributed.keys.impl.SparseMatrixKey;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseDistributedMatrixStorage;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledVectorDouble;
import org.apache.ignite.ml.trees.BaseDecisionTreeTest;
import org.apache.ignite.ml.trees.SplitDataGenerator;
import org.apache.ignite.ml.trees.models.DecisionTreeModel;
import org.apache.ignite.ml.trees.trainers.columnbased.BiIndex;
import org.apache.ignite.ml.trees.trainers.columnbased.BiIndexedCacheColumnDecisionTreeTrainerInput;
import org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer;
import org.apache.ignite.ml.trees.trainers.columnbased.MatrixColumnDecisionTreeTrainerInput;
import org.apache.ignite.ml.trees.trainers.columnbased.caches.ContextCache;
import org.apache.ignite.ml.trees.trainers.columnbased.caches.FeaturesCache;
import org.apache.ignite.ml.trees.trainers.columnbased.caches.ProjectionsCache;
import org.apache.ignite.ml.trees.trainers.columnbased.caches.SplitCache;
import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.ContinuousSplitCalculators;
import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.GiniSplitCalculator;
import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.VarianceSplitCalculator;
import org.apache.ignite.ml.trees.trainers.columnbased.regcalcs.RegionCalculators;
import org.apache.ignite.ml.util.MnistUtils;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.log4j.Level;
import org.junit.Assert;

/**
 * Various benchmarks for hand runs.
 */
public class ColumnDecisionTreeTrainerBenchmark extends BaseDecisionTreeTest {
    /** Name of the property specifying path to training set images. */
    private static final String PROP_TRAINING_IMAGES = "mnist.training.images";

    /** Name of property specifying path to training set labels. */
    private static final String PROP_TRAINING_LABELS = "mnist.training.labels";

    /** Name of property specifying path to test set images. */
    private static final String PROP_TEST_IMAGES = "mnist.test.images";

    /** Name of property specifying path to test set labels. */
    private static final String PROP_TEST_LABELS = "mnist.test.labels";

    /** Function to approximate. */
    private static final Function<Vector, Double> f1 = v -> v.get(0) * v.get(0) + 2 * Math.sin(v.get(1)) + v.get(2);

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 6000000;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName,
        IgniteTestResources rsrcs) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName, rsrcs);
        // We do not need any extra event types.
        configuration.setIncludeEventTypes();
        configuration.setPeerClassLoadingEnabled(false);

        resetLog4j(Level.INFO, false, GridCacheProcessor.class.getPackage().getName());

        return configuration;
    }

    /**
     * This test is for manual run only.
     * To run this test rename this method so it starts from 'test'.
     */
    public void tstCacheMixed() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        int ptsPerReg = 150;
        int featCnt = 10;

        HashMap<Integer, Integer> catsInfo = new HashMap<>();
        catsInfo.put(1, 3);

        Random rnd = new Random(12349L);

        SplitDataGenerator<DenseLocalOnHeapVector> gen = new SplitDataGenerator<>(
            featCnt, catsInfo, () -> new DenseLocalOnHeapVector(featCnt + 1), rnd).
            split(0, 1, new int[] {0, 2}).
            split(1, 0, -10.0).
            split(0, 0, 0.0);

        testByGenStreamerLoad(ptsPerReg, catsInfo, gen, rnd);
    }

    /**
     * Run decision tree classifier on MNIST using bi-indexed cache as a storage for dataset.
     * To run this test rename this method so it starts from 'test'.
     *
     * @throws IOException In case of loading MNIST dataset errors.
     */
    public void tstMNISTBiIndexedCache() throws IOException {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        int ptsCnt = 40_000;
        int featCnt = 28 * 28;

        Properties props = loadMNISTProperties();

        Stream<DenseLocalOnHeapVector> trainingMnistStream = MnistUtils.mnist(props.getProperty(PROP_TRAINING_IMAGES), props.getProperty(PROP_TRAINING_LABELS), new Random(123L), ptsCnt);
        Stream<DenseLocalOnHeapVector> testMnistStream = MnistUtils.mnist(props.getProperty(PROP_TEST_IMAGES), props.getProperty(PROP_TEST_LABELS), new Random(123L), 10_000);

        IgniteCache<BiIndex, Double> cache = createBiIndexedCache();

        loadVectorsIntoBiIndexedCache(cache.getName(), trainingMnistStream.iterator(), featCnt + 1);

        ColumnDecisionTreeTrainer<GiniSplitCalculator.GiniData> trainer =
            new ColumnDecisionTreeTrainer<>(10, ContinuousSplitCalculators.GINI.apply(ignite), RegionCalculators.GINI, RegionCalculators.MOST_COMMON, ignite);

        X.println("Training started.");
        long before = System.currentTimeMillis();
        DecisionTreeModel mdl = trainer.train(new BiIndexedCacheColumnDecisionTreeTrainerInput(cache, new HashMap<>(), ptsCnt, featCnt));
        X.println("Training finished in " + (System.currentTimeMillis() - before));

        IgniteTriFunction<Model<Vector, Double>, Stream<IgniteBiTuple<Vector, Double>>, Function<Double, Double>, Double> mse = Estimators.errorsPercentage();
        Double accuracy = mse.apply(mdl, testMnistStream.map(v -> new IgniteBiTuple<>(v.viewPart(0, featCnt), v.getX(featCnt))), Function.identity());
        X.println("Errors percentage: " + accuracy);

        Assert.assertEquals(0, SplitCache.getOrCreate(ignite).size());
        Assert.assertEquals(0, FeaturesCache.getOrCreate(ignite).size());
        Assert.assertEquals(0, ContextCache.getOrCreate(ignite).size());
        Assert.assertEquals(0, ProjectionsCache.getOrCreate(ignite).size());
    }

    /**
     * Run decision tree classifier on MNIST using sparse distributed matrix as a storage for dataset.
     * To run this test rename this method so it starts from 'test'.
     *
     * @throws IOException In case of loading MNIST dataset errors.
     */
    public void tstMNISTSparseDistributedMatrix() throws IOException {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        int ptsCnt = 30_000;
        int featCnt = 28 * 28;

        Properties props = loadMNISTProperties();

        Stream<DenseLocalOnHeapVector> trainingMnistStream = MnistUtils.mnist(props.getProperty(PROP_TRAINING_IMAGES), props.getProperty(PROP_TRAINING_LABELS), new Random(123L), ptsCnt);
        Stream<DenseLocalOnHeapVector> testMnistStream = MnistUtils.mnist(props.getProperty(PROP_TEST_IMAGES), props.getProperty(PROP_TEST_LABELS), new Random(123L), 10_000);

        SparseDistributedMatrix m = new SparseDistributedMatrix(ptsCnt, featCnt + 1, StorageConstants.COLUMN_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        SparseDistributedMatrixStorage sto = (SparseDistributedMatrixStorage)m.getStorage();

        loadVectorsIntoSparseDistributedMatrixCache(sto.cache().getName(), sto.getUUID(), trainingMnistStream.iterator(), featCnt + 1);

        ColumnDecisionTreeTrainer<GiniSplitCalculator.GiniData> trainer =
            new ColumnDecisionTreeTrainer<>(10, ContinuousSplitCalculators.GINI.apply(ignite), RegionCalculators.GINI, RegionCalculators.MOST_COMMON, ignite);

        X.println("Training started");
        long before = System.currentTimeMillis();
        DecisionTreeModel mdl = trainer.train(new MatrixColumnDecisionTreeTrainerInput(m, new HashMap<>()));
        X.println("Training finished in " + (System.currentTimeMillis() - before));

        IgniteTriFunction<Model<Vector, Double>, Stream<IgniteBiTuple<Vector, Double>>, Function<Double, Double>, Double> mse = Estimators.errorsPercentage();
        Double accuracy = mse.apply(mdl, testMnistStream.map(v -> new IgniteBiTuple<>(v.viewPart(0, featCnt), v.getX(featCnt))), Function.identity());
        X.println("Errors percentage: " + accuracy);

        Assert.assertEquals(0, SplitCache.getOrCreate(ignite).size());
        Assert.assertEquals(0, FeaturesCache.getOrCreate(ignite).size());
        Assert.assertEquals(0, ContextCache.getOrCreate(ignite).size());
        Assert.assertEquals(0, ProjectionsCache.getOrCreate(ignite).size());
    }

    /** Load properties for MNIST tests. */
    private static Properties loadMNISTProperties() throws IOException {
        Properties res = new Properties();

        InputStream is = ColumnDecisionTreeTrainerBenchmark.class.getClassLoader().getResourceAsStream("manualrun/trees/columntrees.manualrun.properties");

        res.load(is);

        return res;
    }

    /** */
    private void testByGenStreamerLoad(int ptsPerReg, HashMap<Integer, Integer> catsInfo,
        SplitDataGenerator<DenseLocalOnHeapVector> gen, Random rnd) {

        List<IgniteBiTuple<Integer, DenseLocalOnHeapVector>> lst = gen.
            points(ptsPerReg, (i, rn) -> i).
            collect(Collectors.toList());

        int featCnt = gen.featuresCnt();

        Collections.shuffle(lst, rnd);

        int numRegs = gen.regsCount();

        SparseDistributedMatrix m = new SparseDistributedMatrix(numRegs * ptsPerReg, featCnt + 1, StorageConstants.COLUMN_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        IgniteFunction<DoubleStream, Double> regCalc = s -> s.average().orElse(0.0);

        Map<Integer, List<LabeledVectorDouble>> byRegion = new HashMap<>();

        SparseDistributedMatrixStorage sto = (SparseDistributedMatrixStorage)m.getStorage();
        long before = System.currentTimeMillis();
        X.println("Batch loading started...");
        loadVectorsIntoSparseDistributedMatrixCache(sto.cache().getName(), sto.getUUID(), gen.
            points(ptsPerReg, (i, rn) -> i).map(IgniteBiTuple::get2).iterator(), featCnt + 1);
        X.println("Batch loading took " + (System.currentTimeMillis() - before) + " ms.");

        for (IgniteBiTuple<Integer, DenseLocalOnHeapVector> bt : lst) {
            byRegion.putIfAbsent(bt.get1(), new LinkedList<>());
            byRegion.get(bt.get1()).add(asLabeledVector(bt.get2().getStorage().data()));
        }

        ColumnDecisionTreeTrainer<VarianceSplitCalculator.VarianceData> trainer =
            new ColumnDecisionTreeTrainer<>(2, ContinuousSplitCalculators.VARIANCE, RegionCalculators.VARIANCE, regCalc, ignite);

        before = System.currentTimeMillis();
        DecisionTreeModel mdl = trainer.train(new MatrixColumnDecisionTreeTrainerInput(m, catsInfo));

        X.println("Training took: " + (System.currentTimeMillis() - before) + " ms.");

        byRegion.keySet().forEach(k -> {
            LabeledVectorDouble sp = byRegion.get(k).get(0);
            Tracer.showAscii(sp.features());
            X.println("Predicted value and label [pred=" + mdl.predict(sp.features()) + ", label=" + sp.doubleLabel() + "]");
            assert mdl.predict(sp.features()) == sp.doubleLabel();
        });
    }

    /**
     * Test decision tree regression.
     * To run this test rename this method so it starts from 'test'.
     */
    public void tstF1() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        int ptsCnt = 10000;
        Map<Integer, double[]> ranges = new HashMap<>();

        ranges.put(0, new double[] {-100.0, 100.0});
        ranges.put(1, new double[] {-100.0, 100.0});
        ranges.put(2, new double[] {-100.0, 100.0});

        int featCnt = 100;
        double[] defRng = {-1.0, 1.0};

        Vector[] trainVectors = vecsFromRanges(ranges, featCnt, defRng, new Random(123L), ptsCnt, f1);

        SparseDistributedMatrix m = new SparseDistributedMatrix(ptsCnt, featCnt + 1, StorageConstants.COLUMN_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        SparseDistributedMatrixStorage sto = (SparseDistributedMatrixStorage)m.getStorage();

        loadVectorsIntoSparseDistributedMatrixCache(sto.cache().getName(), sto.getUUID(), Arrays.stream(trainVectors).iterator(), featCnt + 1);

        IgniteFunction<DoubleStream, Double> regCalc = s -> s.average().orElse(0.0);

        ColumnDecisionTreeTrainer<VarianceSplitCalculator.VarianceData> trainer =
            new ColumnDecisionTreeTrainer<>(10, ContinuousSplitCalculators.VARIANCE, RegionCalculators.VARIANCE, regCalc, ignite);

        X.println("Training started.");
        long before = System.currentTimeMillis();
        DecisionTreeModel mdl = trainer.train(new MatrixColumnDecisionTreeTrainerInput(m, new HashMap<>()));
        X.println("Training finished in: " + (System.currentTimeMillis() - before) + " ms.");

        Vector[] testVectors = vecsFromRanges(ranges, featCnt, defRng, new Random(123L), 20, f1);

        IgniteTriFunction<Model<Vector, Double>, Stream<IgniteBiTuple<Vector, Double>>, Function<Double, Double>, Double> mse = Estimators.MSE();
        Double accuracy = mse.apply(mdl, Arrays.stream(testVectors).map(v -> new IgniteBiTuple<>(v.viewPart(0, featCnt), v.getX(featCnt))), Function.identity());
        X.println("MSE: " + accuracy);
    }

    /**
     * Load vectors into sparse distributed matrix.
     *
     * @param cacheName Name of cache where matrix is stored.
     * @param uuid UUID of matrix.
     * @param iter Iterator over vectors.
     * @param vectorSize size of vectors.
     */
    private void loadVectorsIntoSparseDistributedMatrixCache(String cacheName, UUID uuid,
        Iterator<? extends org.apache.ignite.ml.math.Vector> iter, int vectorSize) {
        try (IgniteDataStreamer<SparseMatrixKey, Map<Integer, Double>> streamer =
                 Ignition.localIgnite().dataStreamer(cacheName)) {
            int sampleIdx = 0;
            streamer.allowOverwrite(true);

            streamer.receiver(StreamTransformer.from((e, arg) -> {
                Map<Integer, Double> val = e.getValue();

                if (val == null)
                    val = new Int2DoubleOpenHashMap();

                val.putAll((Map<Integer, Double>)arg[0]);

                e.setValue(val);

                return null;
            }));

            // Feature index -> (sample index -> value)
            Map<Integer, Map<Integer, Double>> batch = new HashMap<>();
            IntStream.range(0, vectorSize).forEach(i -> batch.put(i, new HashMap<>()));
            int batchSize = 1000;

            while (iter.hasNext()) {
                org.apache.ignite.ml.math.Vector next = iter.next();

                for (int i = 0; i < vectorSize; i++)
                    batch.get(i).put(sampleIdx, next.getX(i));

                X.println("Sample index: " + sampleIdx);
                if (sampleIdx % batchSize == 0) {
                    batch.keySet().forEach(fi -> streamer.addData(new SparseMatrixKey(fi, uuid, fi), batch.get(fi)));
                    IntStream.range(0, vectorSize).forEach(i -> batch.put(i, new HashMap<>()));
                }
                sampleIdx++;
            }
            if (sampleIdx % batchSize != 0) {
                batch.keySet().forEach(fi -> streamer.addData(new SparseMatrixKey(fi, uuid, fi), batch.get(fi)));
                IntStream.range(0, vectorSize).forEach(i -> batch.put(i, new HashMap<>()));
            }
        }
    }

    /**
     * Load vectors into bi-indexed cache.
     *
     * @param cacheName Name of cache.
     * @param iter Iterator over vectors.
     * @param vectorSize size of vectors.
     */
    private void loadVectorsIntoBiIndexedCache(String cacheName,
        Iterator<? extends org.apache.ignite.ml.math.Vector> iter, int vectorSize) {
        try (IgniteDataStreamer<BiIndex, Double> streamer =
                 Ignition.localIgnite().dataStreamer(cacheName)) {
            int sampleIdx = 0;

            streamer.perNodeBufferSize(10000);

            while (iter.hasNext()) {
                org.apache.ignite.ml.math.Vector next = iter.next();

                for (int i = 0; i < vectorSize; i++)
                    streamer.addData(new BiIndex(sampleIdx, i), next.getX(i));

                sampleIdx++;

                if (sampleIdx % 1000 == 0)
                    System.out.println("Loaded: " + sampleIdx + " vectors.");
            }
        }
    }

    /**
     * Create bi-indexed cache for tests.
     *
     * @return Bi-indexed cache.
     */
    private IgniteCache<BiIndex, Double> createBiIndexedCache() {
        CacheConfiguration<BiIndex, Double> cfg = new CacheConfiguration<>();

        // Write to primary.
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

        // Atomic transactions only.
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        // No eviction.
        cfg.setEvictionPolicy(null);

        // No copying of values.
        cfg.setCopyOnRead(false);

        // Cache is partitioned.
        cfg.setCacheMode(CacheMode.PARTITIONED);

        cfg.setBackups(0);

        cfg.setName("TMP_BI_INDEXED_CACHE");

        return Ignition.localIgnite().getOrCreateCache(cfg);
    }

    /** */
    private Vector[] vecsFromRanges(Map<Integer, double[]> ranges, int featCnt, double[] defRng, Random rnd, int ptsCnt,
        Function<Vector, Double> f) {
        int vs = featCnt + 1;
        DenseLocalOnHeapVector[] res = new DenseLocalOnHeapVector[ptsCnt];
        for (int pt = 0; pt < ptsCnt; pt++) {
            DenseLocalOnHeapVector v = new DenseLocalOnHeapVector(vs);
            for (int i = 0; i < featCnt; i++) {
                double[] range = ranges.getOrDefault(i, defRng);
                double from = range[0];
                double to = range[1];
                double rng = to - from;

                v.setX(i, rnd.nextDouble() * rng);
            }
            v.setX(featCnt, f.apply(v));
            res[pt] = v;
        }

        return res;
    }
}
