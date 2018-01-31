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

package org.apache.ignite.examples.ml.trees;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
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
import org.apache.ignite.examples.ExampleNodeStartup;
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
 * <p>
 * Example of usage of decision trees algorithm for MNIST dataset
 * (it can be found here: http://yann.lecun.com/exdb/mnist/). </p>
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.</p>
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.</p>
 * <p>
 * It is recommended to start at least one node prior to launching this example if you intend
 * to run it with default memory settings.</p>
 * <p>
 * This example should be run with program arguments, for example
 * -cfg examples/config/example-ignite.xml.</p>
 * <p>
 * -cfg specifies path to a config path.</p>
 */
public class DecisionTreesExample {
    /** Name of parameter specifying path of Ignite config. */
    private static final String CONFIG = "cfg";

    /** Default config path. */
    private static final String DEFAULT_CONFIG = "examples/config/example-ignite.xml";

    private static String MNIST_DIR = "examples/src/main/resources/";

    private static String MNIST_TRAIN_IMAGES = "train_images";

    private static String MNIST_TRAIN_LABELS = "train_labels";

    private static String MNIST_TEST_IMAGES = "test_images";

    private static String MNIST_TEST_LABELS = "test_labels";

    /**
     * Launches example.
     *
     * @param args Program arguments.
     */
    public static void main(String[] args) throws IOException {
        String igniteCfgPath;

        CommandLineParser parser = new BasicParser();

        String trainingImagesPath;
        String trainingLabelsPath;

        String testImagesPath;
        String testLabelsPath;

        Map<String, String> mnistPaths = new HashMap<>();

        mnistPaths.put(MNIST_TRAIN_IMAGES, "train-images-idx3-ubyte");
        mnistPaths.put(MNIST_TRAIN_LABELS, "train-labels-idx1-ubyte");
        mnistPaths.put(MNIST_TEST_IMAGES, "t10k-images-idx3-ubyte");
        mnistPaths.put(MNIST_TEST_LABELS, "t10k-labels-idx1-ubyte");

        if (!getMNIST(mnistPaths.values())) {
            System.out.println(">>> You should have MNIST dataset in " + MNIST_DIR + " to run this example.");
            return;
        }

        try {
            // Parse the command line arguments.
            CommandLine line = parser.parse(buildOptions(), args);

            trainingImagesPath = MNIST_DIR + "/" + mnistPaths.get(MNIST_TRAIN_IMAGES);
            trainingLabelsPath = MNIST_DIR + "/" + mnistPaths.get(MNIST_TRAIN_LABELS);
            testImagesPath = MNIST_DIR + "/" + mnistPaths.get(MNIST_TEST_IMAGES);
            testLabelsPath = MNIST_DIR + "/" + mnistPaths.get(MNIST_TEST_LABELS);
            igniteCfgPath = line.getOptionValue(CONFIG, DEFAULT_CONFIG);
        }
        catch (ParseException e) {
            e.printStackTrace();
            return;
        }

        try (Ignite ignite = Ignition.start(igniteCfgPath)) {
            IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

            int ptsCnt = 60000;
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
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean getMNIST(Collection<String> mnistPaths) throws IOException {
        List<String> missing = mnistPaths.stream().filter(f -> IgniteUtils.resolveIgnitePath(f) != null).collect(Collectors.toList());

        if (!missing.isEmpty()) {
            System.out.println(">>> You have not fully downloaded MNIST dataset in directory " + MNIST_DIR + ", do you want it to be downloaded? [y]/n");
            Scanner s = new Scanner(System.in);
            String str = s.nextLine();

            if (!str.isEmpty() || str.toLowerCase().equals("y"))
                return false;
        }

        for (String s : missing) {
            String f = s + ".gz";
            System.out.println("Downloading " + f + "...");
            URL website = new URL("http://yann.lecun.com/exdb/mnist/" + f);
            ReadableByteChannel rbc = Channels.newChannel(website.openStream());
            FileOutputStream fos = new FileOutputStream(MNIST_DIR + "/" + f);
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            System.out.println("Done.");

            System.out.println("Unzipping " + f + "...");
            unzip(MNIST_DIR + "/" + f, MNIST_DIR + "/" + s);
            System.out.println("Done.");
        }

        return true;
    }

    private static void unzip(String input, String output) throws IOException {
        byte[] buf = new byte[1024];

        try (GZIPInputStream gis = new GZIPInputStream(new FileInputStream(input));
             FileOutputStream out = new FileOutputStream(output)) {
            int sz;
            while ((sz = gis.read(buf)) > 0)
                out.write(buf, 0, sz);
        }
    }

    /**
     * Build cli options.
     */
    @NotNull private static Options buildOptions() {
        Options options = new Options();

        Option configOpt = OptionBuilder.withArgName(CONFIG).withLongOpt(CONFIG).hasArg()
            .withDescription("Path to the config.")
            .isRequired(false).create();

        options.addOption(configOpt);

        return options;
    }

    /**
     * Creates cache where data for training is stored.
     *
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
     *
     * @param cacheName Name of cache.
     * @param vectorsIterator Iterator over vectors to load.
     * @param vectorSize Size of vector.
     * @param ignite Ignite instance.
     */
    private static void loadVectorsIntoBiIndexedCache(String cacheName, Iterator<? extends Vector> vectorsIterator,
        int vectorSize, Ignite ignite) {
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
