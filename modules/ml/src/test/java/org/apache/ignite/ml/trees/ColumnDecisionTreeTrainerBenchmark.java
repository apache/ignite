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

package org.apache.ignite.ml.trees;

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import java.io.IOException;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.estimators.Estimators;
import org.apache.ignite.ml.math.*;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseDistributedMatrixStorage;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledVectorDouble;
import org.apache.ignite.ml.trees.models.DecisionTreeModel;
import org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeMatrixInput;
import org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer;
import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.ContinuousSplitCalculators;
import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.VarianceSplitCalculator;
import org.apache.ignite.ml.trees.trainers.columnbased.regcalcs.RegionCalculators;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

/**
 * Various benchmarks for hand runs.
 */
public class ColumnDecisionTreeTrainerBenchmark extends BaseDecisionTreeTest {
    private static Function<Vector, Double> f1 = v -> v.get(0) * v.get(0) + 2 * Math.sin(v.get(1)) + v.get(2);

    @Override protected long getTestTimeout() {
        return 6000000;
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName,
        IgniteTestResources rsrcs) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName, rsrcs);
        configuration.setIncludeEventTypes();
        return configuration;
    }

    /**
     * This test is for manual run only.
     */
    @Ignore
    @Test
    public void destCacheMixed() {
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

    @Ignore
    @Test
    public void testMNIST() throws IOException {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        int ptsCnt = 20000;
        int featCnt = 28 * 28;

        Stream<DenseLocalOnHeapVector> trainingMnistStream = ReadMnistData.mnist("/home/enny/Downloads/train-images-idx3-ubyte", "/home/enny/Downloads/train-labels-idx1-ubyte", ptsCnt);
        Stream<DenseLocalOnHeapVector> testMnistStream = ReadMnistData.mnist("/home/enny/Downloads/t10k-images.idx3-ubyte", "/home/enny/Downloads/t10k-labels.idx1-ubyte", ptsCnt / 10);

        SparseDistributedMatrix m = new SparseDistributedMatrix(ptsCnt, featCnt + 1, StorageConstants.COLUMN_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        SparseDistributedMatrixStorage sto = (SparseDistributedMatrixStorage) m.getStorage();

        loadVectorsIntoCache(sto.cache().getName(), sto.getUUID(), trainingMnistStream.iterator(), featCnt + 1);

        ColumnDecisionTreeTrainer<VarianceSplitCalculator.VarianceData> trainer =
            new ColumnDecisionTreeTrainer<>(10, ContinuousSplitCalculators.VARIANCE, RegionCalculators.VARIANCE, RegionCalculators.MOST_COMMON);

        System.out.println(">>> Training started");
        long before = System.currentTimeMillis();
        DecisionTreeModel mdl = trainer.train(new ColumnDecisionTreeMatrixInput(m, new HashMap<>()));
        System.out.println(">>> Training finished in " + (System.currentTimeMillis() - before));

        IgniteTriFunction<Model<Vector, Double>, Stream<IgniteBiTuple<Vector, Double>>, Function<Double, Double>, Double> mse = Estimators.errorsPercentage();
        Double accuracy = mse.apply(mdl, testMnistStream.map(v -> new IgniteBiTuple<>(v.viewPart(0, featCnt), v.getX(featCnt))), Function.identity());
        System.out.println(">>> Errs percentage: " + accuracy);

        trainer.destroy();
    }

    @Ignore
    @Test
    public void testF1() {
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

        SparseDistributedMatrixStorage sto = (SparseDistributedMatrixStorage) m.getStorage();

        loadVectorsIntoCache(sto.cache().getName(), sto.getUUID(), Arrays.stream(trainVectors).iterator(), featCnt + 1);

        IgniteFunction<DoubleStream, Double> regCalc = s -> s.average().orElse(0.0);

        ColumnDecisionTreeTrainer<VarianceSplitCalculator.VarianceData> trainer =
            new ColumnDecisionTreeTrainer<>(11, new VarianceSplitCalculator(), RegionCalculators.VARIANCE, regCalc);

        System.out.println(">>> Training started");
        long before = System.currentTimeMillis();
        DecisionTreeModel mdl = trainer.train(new ColumnDecisionTreeMatrixInput(m, new HashMap<>()));
        System.out.println(">>> Training finished in " + (System.currentTimeMillis() - before));

        Vector[] testVectors = vecsFromRanges(ranges, featCnt, defRng, new Random(123L), 20, f1);

        IgniteTriFunction<Model<Vector, Double>, Stream<IgniteBiTuple<Vector, Double>>, Function<Double, Double>, Double> mse = Estimators.MSE();
        Double accuracy = mse.apply(mdl, Arrays.stream(testVectors).map(v -> new IgniteBiTuple<>(v.viewPart(0, featCnt), v.getX(featCnt))), Function.identity());
        System.out.println(">>> MSE: " + accuracy);

        trainer.destroy();
    }

    private void testByGenStreamerLoad(int ptsPerReg, HashMap<Integer, Integer> catsInfo,
                           SplitDataGenerator<DenseLocalOnHeapVector> gen, Random rnd) {

        List<IgniteBiTuple<Integer, DenseLocalOnHeapVector>> lst = gen.
                points(ptsPerReg, (i, rn) -> i).
                collect(Collectors.toList());

        int featCnt = gen.featCnt();

        Collections.shuffle(lst, rnd);

        int numRegs = gen.numRegs();

        SparseDistributedMatrix m = new SparseDistributedMatrix(numRegs * ptsPerReg, featCnt + 1, StorageConstants.COLUMN_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        IgniteFunction<DoubleStream, Double> regCalc = s -> s.average().orElse(0.0);

        Map<Integer, List<LabeledVectorDouble>> byRegion = new HashMap<>();

        SparseDistributedMatrixStorage sto = (SparseDistributedMatrixStorage) m.getStorage();
        long before = System.currentTimeMillis();
        System.out.println(">>> Batch loading started...");
        loadVectorsIntoCache(sto.cache().getName(), sto.getUUID(), gen.
                points(ptsPerReg, (i, rn) -> i).map(IgniteBiTuple::get2).iterator(), featCnt + 1);
        System.out.println(">>> Batch loading took " + (System.currentTimeMillis() - before) + " ms.");

        for (IgniteBiTuple<Integer, DenseLocalOnHeapVector> bt : lst) {
            byRegion.putIfAbsent(bt.get1(), new LinkedList<>());
            byRegion.get(bt.get1()).add(asLabeledVector(bt.get2().getStorage().data()));
        }

        ColumnDecisionTreeTrainer<VarianceSplitCalculator.VarianceData> trainer =
                new ColumnDecisionTreeTrainer<>(2, new VarianceSplitCalculator(), RegionCalculators.VARIANCE, regCalc);

        before = System.currentTimeMillis();
        DecisionTreeModel mdl = trainer.train(new ColumnDecisionTreeMatrixInput(m, catsInfo));

        System.out.println("Took time(ms): " + (System.currentTimeMillis() - before));

        byRegion.keySet().stream().forEach(k -> {
            LabeledVectorDouble sp = byRegion.get(k).get(0);
            Tracer.showAscii(sp.vector());
            System.out.println("Prediction: " + mdl.predict(sp.vector()) + "label: " + sp.doubleLabel());
            assert mdl.predict(sp.vector()) == sp.doubleLabel();
        });

        trainer.destroy();
    }

    private void loadVectorsIntoCache(String cacheName, IgniteUuid uuid, Iterator<? extends org.apache.ignite.ml.math.Vector> str, int vectorSize) {
        try (IgniteDataStreamer<IgniteBiTuple<Integer, IgniteUuid>, Map<Integer, Double>> streamer =
                     Ignition.localIgnite().dataStreamer(cacheName)) {
            int sampleIdx = 0;
            streamer.allowOverwrite(true);

            streamer.receiver(StreamTransformer.from((e, arg) -> {
                Map<Integer, Double> value = e.getValue();

                if (value == null)
                    value = new Int2DoubleOpenHashMap();

                value.putAll((Map<Integer, Double>)arg[0]);

                e.setValue(value);

                return null;
            }));

            // Feature index -> (sample index -> value)
            Map<Integer, Map<Integer, Double>> batch = new HashMap<>();
            IntStream.range(0, vectorSize).forEach(i -> batch.put(i, new HashMap<>()));
            int batchSize = 100;

            while (str.hasNext()) {
                org.apache.ignite.ml.math.Vector next = str.next();

                for (int i = 0; i < vectorSize; i++)
                    batch.get(i).put(sampleIdx, next.getX(i));

                System.out.println(sampleIdx);
                if (sampleIdx % batchSize == 0) {
                    batch.keySet().forEach(fi -> {
                        streamer.addData(new IgniteBiTuple<>(fi, uuid), batch.get(fi));
                    });
                    IntStream.range(0, vectorSize).forEach(i -> batch.put(i, new HashMap<>()));
                }
                sampleIdx++;
            }
            if (sampleIdx % batchSize != 0) {
                batch.keySet().forEach(fi -> {
                    streamer.addData(new IgniteBiTuple<>(fi, uuid), batch.get(fi));
                });
                IntStream.range(0, vectorSize).forEach(i -> batch.put(i, new HashMap<>()));
            }
        }
    }

    private Vector[] vecsFromRanges(Map<Integer, double[]> ranges, int featCnt, double[] defRng, Random rnd, int ptsCnt, Function<Vector, Double> f) {
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
