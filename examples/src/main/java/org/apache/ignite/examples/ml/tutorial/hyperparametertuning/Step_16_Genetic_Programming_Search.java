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

package org.apache.ignite.examples.ml.tutorial.hyperparametertuning;

import java.io.FileNotFoundException;
import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.tutorial.TitanicUtils;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.encoding.EncoderTrainer;
import org.apache.ignite.ml.preprocessing.encoding.EncoderType;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.selection.cv.CrossValidation;
import org.apache.ignite.ml.selection.cv.CrossValidationResult;
import org.apache.ignite.ml.selection.paramgrid.EvolutionOptimizationStrategy;
import org.apache.ignite.ml.selection.paramgrid.ParamGrid;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

/**
 * To choose the best hyper-parameters the cross-validation with {@link ParamGrid} will be used in this example.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data (based on Titanic passengers data).</p>
 * <p>
 * After that it defines how to split the data to train and test sets and configures preprocessors that extract features
 * from an upstream data and perform other desired changes over the extracted data.</p>
 * <p>
 * Then, it tunes hyper-parameters with K-fold Cross-Validation on the split training set and trains the model based on the
 * processed data using decision tree classification and the obtained hyperparams.</p>
 * <p>
 * Finally, this example uses {@link Evaluator} functionality to compute metrics from predictions.</p>
 * <p>
 * The purpose of cross-validation is model checking, not model building.</p>
 * <p>
 * We train {@code k} different models.</p>
 * <p>
 * They differ in that {@code 1/(k-1)}th of the training data is exchanged against other cases.</p>
 * <p>
 * These models are sometimes called surrogate models because the (average) performance measured for these models is
 * taken as a surrogate of the performance of the model trained on all cases.</p>
 * <p>
 * All scenarios are described there: https://sebastianraschka.com/faq/docs/evaluate-a-model.html</p>
 */
public class Step_16_Genetic_Programming_Search {
    /**
     * Run example.
     */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Tutorial step 16 (Genetic Programming) example started.");

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            try {
                IgniteCache<Integer, Vector> dataCache = TitanicUtils.readPassengers(ignite);

                // Extracts "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare".
                final Vectorizer<Integer, Vector, Integer, Double> vectorizer
                    = new DummyVectorizer<Integer>(0, 3, 4, 5, 6, 8, 10).labeled(1);

                TrainTestSplit<Integer, Vector> split = new TrainTestDatasetSplitter<Integer, Vector>()
                    .split(0.75);

                Preprocessor<Integer, Vector> strEncoderPreprocessor = new EncoderTrainer<Integer, Vector>()
                    .withEncoderType(EncoderType.STRING_ENCODER)
                    .withEncodedFeature(1)
                    .withEncodedFeature(6)
                    .fit(ignite,
                        dataCache,
                        vectorizer
                    );

                Preprocessor<Integer, Vector> imputingPreprocessor = new ImputerTrainer<Integer, Vector>()
                    .fit(ignite,
                        dataCache,
                        strEncoderPreprocessor
                    );

                Preprocessor<Integer, Vector> minMaxScalerPreprocessor = new MinMaxScalerTrainer<Integer, Vector>()
                    .fit(
                        ignite,
                        dataCache,
                        imputingPreprocessor
                    );

                NormalizationTrainer<Integer, Vector> normalizationTrainer = new NormalizationTrainer<Integer, Vector>()
                    .withP(1);

                Preprocessor<Integer, Vector> normalizationPreprocessor = normalizationTrainer
                    .fit(
                        ignite,
                        dataCache,
                        minMaxScalerPreprocessor
                    );

                // Tune hyper-parameters with K-fold Cross-Validation on the split training set.

                DecisionTreeClassificationTrainer trainerCV = new DecisionTreeClassificationTrainer();

                CrossValidation<DecisionTreeNode, Integer, Vector> scoreCalculator
                    = new CrossValidation<>();

                ParamGrid paramGrid = new ParamGrid()
                    .withParameterSearchStrategy(new EvolutionOptimizationStrategy())
                    .addHyperParam("p", normalizationTrainer::withP, new Double[] {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0})
                    .addHyperParam("maxDeep", trainerCV::withMaxDeep, new Double[] {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0})
                    .addHyperParam("minImpurityDecrease", trainerCV::withMinImpurityDecrease, new Double[] {0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});

                scoreCalculator
                    .withIgnite(ignite)
                    .withUpstreamCache(dataCache)
                    .withTrainer(trainerCV)
                    .withMetric(MetricName.ACCURACY)
                    .withFilter(split.getTrainFilter())
                    .isRunningOnPipeline(false)
                    .withPreprocessor(normalizationPreprocessor)
                    .withAmountOfFolds(3)
                    .withParamGrid(paramGrid);

                CrossValidationResult crossValidationRes = scoreCalculator.tuneHyperParameters();

                System.out.println("Train with maxDeep: " + crossValidationRes.getBest("maxDeep")
                    + " and minImpurityDecrease: " + crossValidationRes.getBest("minImpurityDecrease"));

                DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer()
                    .withMaxDeep(crossValidationRes.getBest("maxDeep"))
                    .withMinImpurityDecrease(crossValidationRes.getBest("minImpurityDecrease"));

                System.out.println(crossValidationRes);

                System.out.println("Best score: " + Arrays.toString(crossValidationRes.getBestScore()));
                System.out.println("Best hyper params: " + crossValidationRes.getBestHyperParams());
                System.out.println("Best average score: " + crossValidationRes.getBestAvgScore());

                crossValidationRes.getScoringBoard().forEach((hyperParams, score)
                    -> System.out.println("Score " + Arrays.toString(score) + " for hyper params " + hyperParams));

                // Train decision tree model.
                DecisionTreeNode bestMdl = trainer.fit(
                    ignite,
                    dataCache,
                    split.getTrainFilter(),
                    normalizationPreprocessor
                );

                System.out.println("\n>>> Trained model: " + bestMdl);

                double accuracy = Evaluator.evaluate(
                    dataCache, split.getTestFilter(),
                    bestMdl,
                    normalizationPreprocessor,
                    new Accuracy<>()
                );

                System.out.println("\n>>> Accuracy " + accuracy);
                System.out.println("\n>>> Test Error " + (1 - accuracy));

                System.out.println(">>> Tutorial step 16 (Genetic Programming) example completed.");
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        finally {
            System.out.flush();
        }
    }
}
