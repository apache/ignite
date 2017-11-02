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

package org.apache.ignite.examples.ml.math.trees;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.estimators.Estimators;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.trees.models.DecisionTreeModel;
import org.apache.ignite.ml.trees.trainers.columnbased.BiIndex;
import org.apache.ignite.ml.trees.trainers.columnbased.BiIndexedCacheColumnDecisionTreeTrainerInput;
import org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer;
import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.ContinuousSplitCalculators;
import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.GiniSplitCalculator;
import org.apache.ignite.ml.trees.trainers.columnbased.regcalcs.RegionCalculators;
import org.apache.ignite.ml.util.MnistUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Example of usage of decision trees algorithm for MNIST dataset.
 */
public class MNISTExample {
    /** Name of parameter specifying path to training set images. */
    private static final String MNIST_TRAINING_IMAGES_PATH = "ts_i";

    /** Name of parameter specifying path to training set labels. */
    private static final String MNIST_TRAINING_LABELS_PATH = "ts_l";

    /** Name of parameter specifying path to test set images. */
    private static final String MNIST_TEST_IMAGES_PATH = "tss_i";

    /** Name of parameter specifying path to test set labels. */
    private static final String MNIST_TEST_LABELS_PATH = "tss_l";

    /** Name of parameter specifying path of Ignite config. */
    private static final String CONFIG = "cfg";

    /**
     * Launches example.
     * @param args Program arguments.
     */
    public static void main(String[] args) {
        String igniteCfgPath;

        CommandLineParser parser = new BasicParser();

        String trainingImagesPath;
        String trainingLabelsPath;

        String testImagesPath;
        String testLabelsPath;

        try {
            // Parse the command line arguments.
            CommandLine line = parser.parse(buildOptions(), args);

            trainingImagesPath = line.getOptionValue(MNIST_TRAINING_IMAGES_PATH);
            trainingLabelsPath = line.getOptionValue(MNIST_TRAINING_LABELS_PATH);
            testImagesPath = line.getOptionValue(MNIST_TEST_IMAGES_PATH);
            testLabelsPath = line.getOptionValue(MNIST_TEST_LABELS_PATH);
            igniteCfgPath = line.getOptionValue(CONFIG);

            if (line.hasOption("cfg")) {
                igniteCfgPath = line.getOptionValue("cfg");
                System.out.println("Starting with config " + igniteCfgPath);
            }
        }
        catch (ParseException e) {
            e.printStackTrace();
            return;
        }

        try (Ignite ignite = Ignition.start(igniteCfgPath)) {
            IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

            int ptsCnt = 40000;
            int featCnt = 28 * 28;

            Stream<DenseLocalOnHeapVector> trainingMnistStream = MnistUtils.mnist(trainingImagesPath, trainingLabelsPath, new Random(123L), ptsCnt);
            Stream<DenseLocalOnHeapVector> testMnistStream = MnistUtils.mnist(testImagesPath, testLabelsPath, new Random(123L), 10_000);

            IgniteCache<BiIndex, Double> cache = createBiIndexedCache(ignite);

            loadVectorsIntoBiIndexedCache(cache.getName(), trainingMnistStream.iterator(), featCnt + 1, ignite);

            ColumnDecisionTreeTrainer<GiniSplitCalculator.GiniData> trainer =
                new ColumnDecisionTreeTrainer<>(10, ContinuousSplitCalculators.GINI.apply(ignite), RegionCalculators.GINI, RegionCalculators.MOST_COMMON, ignite);

            System.out.println(">>> Training started");
            long before = System.currentTimeMillis();
            DecisionTreeModel mdl = trainer.train(new BiIndexedCacheColumnDecisionTreeTrainerInput(cache, new HashMap<>(), ptsCnt, featCnt));
            System.out.println(">>> Training finished in " + (System.currentTimeMillis() - before));

            IgniteTriFunction<Model<Vector, Double>, Stream<IgniteBiTuple<Vector, Double>>, Function<Double, Double>, Double> mse = Estimators.errorsPercentage();
            Double accuracy = mse.apply(mdl, testMnistStream.map(v -> new IgniteBiTuple<>(v.viewPart(0, featCnt), v.getX(featCnt))), Function.identity());
            System.out.println(">>> Errs percentage: " + accuracy);

            trainer.destroy();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Build cli options.
     */
    @NotNull private static Options buildOptions() {
        Options options = new Options();

        Option trsImagesPathOpt = OptionBuilder.withArgName(MNIST_TRAINING_IMAGES_PATH).withLongOpt(MNIST_TRAINING_IMAGES_PATH).hasArg()
            .withDescription("Path to the MNIST training set.")
            .isRequired(true).create();

        Option trsLabelsPathOpt = OptionBuilder.withArgName(MNIST_TRAINING_LABELS_PATH).withLongOpt(MNIST_TRAINING_LABELS_PATH).hasArg()
            .withDescription("Path to the MNIST training set.")
            .isRequired(true).create();

        Option tssImagesPathOpt = OptionBuilder.withArgName(MNIST_TEST_IMAGES_PATH).withLongOpt(MNIST_TEST_IMAGES_PATH).hasArg()
            .withDescription("Path to the MNIST test set.")
            .isRequired(true).create();

        Option tssLabelsPathOpt = OptionBuilder.withArgName(MNIST_TEST_LABELS_PATH).withLongOpt(MNIST_TEST_LABELS_PATH).hasArg()
            .withDescription("Path to the MNIST test set.")
            .isRequired(true).create();

        Option configOpt = OptionBuilder.withArgName(CONFIG).withLongOpt(CONFIG).hasArg()
            .withDescription("Path to the client config.")
            .isRequired(true).create();

        options.addOption(trsImagesPathOpt);
        options.addOption(trsLabelsPathOpt);
        options.addOption(tssImagesPathOpt);
        options.addOption(tssLabelsPathOpt);
        options.addOption(configOpt);

        return options;
    }

    /**
     * Creates cache where data for training is stored.
     * @param ignite Ignite instance.
     * @return cache where data for training is stored.
     */
    private static IgniteCache<BiIndex, Double> createBiIndexedCache(Ignite ignite) {
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

        return ignite.getOrCreateCache(cfg);
    }

    /**
     * Loads vectors into cache.
     * @param cacheName Name of cache.
     * @param vectorsIterator Iterator over vectors to load.
     * @param vectorSize Size of vector.
     * @param ignite Ignite instance.
     */
    private static void loadVectorsIntoBiIndexedCache(String cacheName, Iterator<? extends Vector> vectorsIterator, int vectorSize, Ignite ignite) {
        try (IgniteDataStreamer<BiIndex, Double> streamer =
                 ignite.dataStreamer(cacheName)) {
            int sampleIdx = 0;

            streamer.perNodeBufferSize(10000);

            while (vectorsIterator.hasNext()) {
                org.apache.ignite.ml.math.Vector next = vectorsIterator.next();

                for (int i = 0; i < vectorSize; i++)
                    streamer.addData(new BiIndex(sampleIdx, i), next.getX(i));

                sampleIdx++;

                if (sampleIdx % 1000 == 0)
                    System.out.println("Loaded " + sampleIdx + " vectors.");
            }
        }
    }
}