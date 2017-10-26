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
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.estimators.Estimators;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseDistributedMatrixStorage;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.trees.models.DecisionTreeModel;
import org.apache.ignite.ml.trees.trainers.columnbased.MatrixColumnDecisionTreeTrainerInput;
import org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer;
import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.ContinuousSplitCalculators;
import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.GiniSplitCalculator;
import org.apache.ignite.ml.trees.trainers.columnbased.regcalcs.RegionCalculators;
import org.apache.ignite.ml.util.MnistUtils;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.thread.IgniteThread;

public class MultinodeMnistLauncher {
    public static void main(String[] args) throws InterruptedException {
        URL cfg = MultinodeMnistLauncher.class.getClassLoader().getResource("mnist-config.xml");
        try (Ignite ignite = Ignition.start(cfg)) {
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(), MultinodeMnistLauncher.class.getSimpleName(), () -> {
                int ptsCnt = 40000;
                int featCnt = 28 * 28;

                Stream<DenseLocalOnHeapVector> trainingMnistStream = null;
                Stream<DenseLocalOnHeapVector> testMnistStream = null;

                try {
                    trainingMnistStream = MnistUtils.mnist("/home/enny/Downloads/train-images-idx3-ubyte", "/home/enny/Downloads/train-labels-idx1-ubyte", new Random(123L), ptsCnt);
                    testMnistStream = MnistUtils.mnist("/home/enny/Downloads/t10k-images.idx3-ubyte", "/home/enny/Downloads/t10k-labels.idx1-ubyte", new Random(123L), 10_000);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }

                SparseDistributedMatrix m = new SparseDistributedMatrix(ptsCnt, featCnt + 1, StorageConstants.COLUMN_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

                SparseDistributedMatrixStorage sto = (SparseDistributedMatrixStorage) m.getStorage();

                loadVectorsIntoSparseDistributedMatrixCache(sto.cache().getName(), sto.getUUID(), trainingMnistStream.iterator(), featCnt + 1);

//        IgniteCache<BiIndex, Double> cache = createBiIndexedCache();

//        loadVectorsIntoBiIndexedCache(cache.getName(), trainingMnistStream.iterator(), featCnt + 1);

                ColumnDecisionTreeTrainer<GiniSplitCalculator.GiniData> trainer =
                    new ColumnDecisionTreeTrainer<>(10, ContinuousSplitCalculators.GINI.apply(ignite), RegionCalculators.GINI, RegionCalculators.MOST_COMMON, ignite);

                System.out.println(">>> Training started");
                long before = System.currentTimeMillis();
//        DecisionTreeModel mdl = trainer.train(new BiIndexedCacheColumnDecisionTreeTrainerInput(cache, new HashMap<>(), ptsCnt, featCnt));
                DecisionTreeModel mdl = trainer.train(new MatrixColumnDecisionTreeTrainerInput(m, new HashMap<>()));
                System.out.println(">>> Training finished in " + (System.currentTimeMillis() - before));

                IgniteTriFunction<Model<Vector, Double>, Stream<IgniteBiTuple<Vector, Double>>, Function<Double, Double>, Double> mse = Estimators.errorsPercentage();
                Double accuracy = mse.apply(mdl, testMnistStream.map(v -> new IgniteBiTuple<>(v.viewPart(0, featCnt), v.getX(featCnt))), Function.identity());
                System.out.println(">>> Errs percentage: " + accuracy);

                trainer.destroy();

            });

            igniteThread.start();

            igniteThread.join();
        }
    }

    private static void loadVectorsIntoSparseDistributedMatrixCache(String cacheName, UUID uuid, Iterator<? extends Vector> str, int vectorSize) {
        try (IgniteDataStreamer<IgniteBiTuple<Integer, UUID>, Map<Integer, Double>> streamer =
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
            int batchSize = 1000;

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
}
