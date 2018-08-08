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

package org.apache.ignite.ml.selection.scoring.evaluator;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.encoding.EncoderTrainer;
import org.apache.ignite.ml.preprocessing.encoding.EncoderType;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.selection.cv.CrossValidation;
import org.apache.ignite.ml.selection.cv.CrossValidationResult;
import org.apache.ignite.ml.selection.paramgrid.ParamGrid;
import org.apache.ignite.ml.selection.scoring.metric.Accuracy;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link Evaluator} that require to start the whole Ignite infrastructure. IMPL NOTE based on
 * Step_8_CV_with_Param_Grid example.
 */
public class EvaluatorTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 3;

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

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /** */
    public void testBasic() throws InterruptedException {
        AtomicReference<Double> actualAccuracy = new AtomicReference<>(null);
        AtomicReference<Double> actualAccuracy2 = new AtomicReference<>(null);
        AtomicReference<CrossValidationResult> res = new AtomicReference<>(null);
        List<double[]> actualScores = new ArrayList<>();

        IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
            EvaluatorTest.class.getSimpleName(), () -> {
            CacheConfiguration<Integer, Object[]> cacheConfiguration = new CacheConfiguration<>();
            cacheConfiguration.setName(UUID.randomUUID().toString());
            cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 10));

            IgniteCache<Integer, Object[]> dataCache = ignite.createCache(cacheConfiguration);

            readPassengers(dataCache);

            // Defines first preprocessor that extracts features from an upstream data.
            // Extracts "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare"
            IgniteBiFunction<Integer, Object[], Object[]> featureExtractor
                = (k, v) -> new Object[] {v[0], v[3], v[4], v[5], v[6], v[8], v[10]};

            IgniteBiFunction<Integer, Object[], Double> lbExtractor = (k, v) -> (double)v[1];

            TrainTestSplit<Integer, Object[]> split = new TrainTestDatasetSplitter<Integer, Object[]>()
                .split(0.75);

            IgniteBiFunction<Integer, Object[], Vector> strEncoderPreprocessor = new EncoderTrainer<Integer, Object[]>()
                .withEncoderType(EncoderType.STRING_ENCODER)
                .encodeFeature(1)
                .encodeFeature(6) // <--- Changed index here
                .fit(ignite,
                    dataCache,
                    featureExtractor
                );

            IgniteBiFunction<Integer, Object[], Vector> imputingPreprocessor = new ImputerTrainer<Integer, Object[]>()
                .fit(ignite,
                    dataCache,
                    strEncoderPreprocessor
                );

            IgniteBiFunction<Integer, Object[], Vector> minMaxScalerPreprocessor = new MinMaxScalerTrainer<Integer, Object[]>()
                .fit(
                    ignite,
                    dataCache,
                    imputingPreprocessor
                );

            IgniteBiFunction<Integer, Object[], Vector> normalizationPreprocessor = new NormalizationTrainer<Integer, Object[]>()
                .withP(2)
                .fit(
                    ignite,
                    dataCache,
                    minMaxScalerPreprocessor
                );

            // Tune hyperparams with K-fold Cross-Validation on the split training set.

            DecisionTreeClassificationTrainer trainerCV = new DecisionTreeClassificationTrainer();

            CrossValidation<DecisionTreeNode, Double, Integer, Object[]> scoreCalculator
                = new CrossValidation<>();

            ParamGrid paramGrid = new ParamGrid()
                .addHyperParam("maxDeep", new Double[] {1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 10.0})
                .addHyperParam("minImpurityDecrease", new Double[] {0.0, 0.25, 0.5});

            CrossValidationResult crossValidationRes = scoreCalculator.score(
                trainerCV,
                new Accuracy<>(),
                ignite,
                dataCache,
                split.getTrainFilter(),
                normalizationPreprocessor,
                lbExtractor,
                3,
                paramGrid
            );

            res.set(crossValidationRes);

            DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer()
                .withMaxDeep(crossValidationRes.getBest("maxDeep"))
                .withMinImpurityDecrease(crossValidationRes.getBest("minImpurityDecrease"));

            crossValidationRes.getScoringBoard().forEach((hyperParams, score) -> actualScores.add(score));

            // Train decision tree model.
            DecisionTreeNode bestMdl = trainer.fit(
                ignite,
                dataCache,
                split.getTrainFilter(),
                normalizationPreprocessor,
                lbExtractor
            );

            double accuracy = Evaluator.evaluate(
                dataCache,
                split.getTestFilter(),
                bestMdl,
                normalizationPreprocessor,
                lbExtractor,
                new Accuracy<>()
            );

            actualAccuracy.set(accuracy);
            actualAccuracy2.set(Evaluator.evaluate(
                dataCache,
                bestMdl,
                normalizationPreprocessor,
                lbExtractor,
                new Accuracy<>()
            ));
        });

        igniteThread.start();

        igniteThread.join();

        assertResults(res.get(), actualScores, actualAccuracy.get(), actualAccuracy2.get());
    }

    /** */
    private void assertResults(CrossValidationResult res, List<double[]> scores, double accuracy, double accuracy2) {
        assertTrue(res.toString().length() > 0);
        assertEquals("Best maxDeep", 1.0, res.getBest("maxDeep"));
        assertEquals("Best minImpurityDecrease", 0.0, res.getBest("minImpurityDecrease"));
        assertArrayEquals("Best score", new double[] {0.6666666666666666, 0.4, 0}, res.getBestScore(), 0);
        assertEquals("Best hyper params size", 2, res.getBestHyperParams().size());
        assertEquals("Best average score", 0.35555555555555557, res.getBestAvgScore());

        assertEquals("Scores amount", 18, scores.size());

        int idx = 0;
        for (double[] actualScore : scores)
            assertEquals("Score size at index " + idx++, 3, actualScore.length);

        assertEquals("Accuracy", 1.0, accuracy);
        assertTrue("Accuracy without filter", accuracy2 > 0.);
    }

    /**
     * Read passengers data.
     *
     * @param cache The ignite cache.
     */
    private void readPassengers(IgniteCache<Integer, Object[]> cache) {
        // IMPL NOTE: pclass;survived;name;sex;age;sibsp;parch;ticket;fare;cabin;embarked;boat;body;homedest
        List<String[]> passengers = Arrays.asList(
            new String[] {
                "1", "1", "Allen, Miss. Elisabeth Walton", "",
                "29", "", "", "24160", "211,3375", "B5", "", "2", "", "St Louis, MO"},
            new String[] {
                "1", "1", "Allison, Master. Hudson Trevor", "male",
                "0,9167", "1", "2", "113781", "151,55", "C22 C26", "S", "11", "", "Montreal, PQ / Chesterville, ON"},
            new String[] {
                "1", "0", "Allison, Miss. Helen Loraine", "female",
                "2", "1", "2", "113781", "151,55", "C22 C26", "S", "", "", "Montreal, PQ / Chesterville, ON"},
            new String[] {
                "1", "0", "Allison, Mr. Hudson Joshua Creighton",
                "male", "30", "1", "2", "113781", "151,55", "C22 C26", "S", "", "135", "Montreal, PQ / Chesterville, ON"},
            new String[] {
                "1", "0", "Allison, Mrs. Hudson J C (Bessie Waldo Daniels)", "female",
                "25", "1", "2", "113781", "151,55", "C22 C26", "S", "", "", "Montreal, PQ / Chesterville, ON"},
            new String[] {
                "1", "1", "Anderson, Mr. Harry", "male",
                "48", "0", "0", "19952", "26,55", "E12", "S", "3", "", "New York, NY"},
            new String[] {
                "1", "1", "Andrews, Miss. Kornelia Theodosia", "female",
                "63", "1", "0", "13502", "77,9583", "D7", "S", "10", "", "Hudson, NY"},
            new String[] {
                "1", "0", "Andrews, Mr. Thomas Jr", "male",
                "39", "0", "0", "112050", "0", "A36", "S", "", "", "Belfast, NI"},
            new String[] {
                "1", "1", "Appleton, Mrs. Edward Dale (Charlotte Lamson)", "female",
                "53", "2", "0", "11769", "51,4792", "C101", "S", "D", "", "Bayside, Queens, NY"},
            new String[] {
                "1", "0", "Artagaveytia, Mr. Ramon", "male",
                "71", "0", "0", "PC 17609", "49,5042", "", "C", "", "22", "Montevideo, Uruguay"});

        int cnt = 1;
        for (String[] details : passengers) {
            Object[] data = new Object[details.length];

            for (int i = 0; i < details.length; i++)
                data[i] = doubleOrString(details[i]);

            cache.put(cnt++, data);
        }
    }

    /** */
    private Object doubleOrString(String data) {
        NumberFormat format = NumberFormat.getInstance(Locale.FRANCE);
        try {
            return data.equals("") ? Double.NaN : Double.valueOf(data);
        }
        catch (java.lang.NumberFormatException e) {

            try {
                return format.parse(data).doubleValue();
            }
            catch (ParseException e1) {
                return data;
            }
        }
    }
}
