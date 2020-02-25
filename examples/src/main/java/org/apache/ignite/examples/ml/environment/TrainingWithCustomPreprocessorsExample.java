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

package org.apache.ignite.examples.ml.environment;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.pipeline.Pipeline;
import org.apache.ignite.ml.pipeline.PipelineMdl;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;

/**
 * This example demonstrates an ability of using custom client classes in cluster in case of absence of these classes on
 * server nodes. Preprocessors (see {@link Preprocessor}, preprocessor trainers (see {@link PreprocessingTrainer} and
 * vectorizers (see {@link Vectorizer}) can be defined in client code and deployed to server nodes during training
 * phase.
 * <p>
 * For demonstrating of deployment abilities of ml-related classes you can run this example with binary build. NOTE:
 * This binary build should be run with copied basic ml libs from optional directory (ignite-ml at least).
 */
public class TrainingWithCustomPreprocessorsExample {
    /**
     * Run example.
     *
     * @param args Command line arguments.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            IgniteCache<Integer, Vector> trainingSet = null;
            try {
                trainingSet = new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.BOSTON_HOUSE_PRICES);

                Vectorizer<Integer, Vector, Integer, Double> basicVectorizer = new DummyVectorizer<Integer>()
                    .labeled(Vectorizer.LabelCoordinate.FIRST);

                Preprocessor<Integer, Vector> imputingPreprocessor = new ImputerTrainer<Integer, Vector>()
                    .fit(ignite, trainingSet, basicVectorizer);

                // In-place definition of custom preprocessor by lambda expression.
                Preprocessor<Integer, Vector> customPreprocessor = (k, v) -> {
                    LabeledVector res = imputingPreprocessor.apply(k, v);
                    double fifthFeature = res.features().get(5);

                    Vector updatedVector = res.features().set(5, fifthFeature > 0 ? Math.log(fifthFeature) : -1);
                    return updatedVector.labeled(res.label());
                };

                Vectorizer9000 customVectorizer = new Vectorizer9000(customPreprocessor);

                PipelineMdl<Integer, Vector> mdl = new Pipeline<Integer, Vector, Integer, Double>()
                    .addVectorizer(customVectorizer)
                    .addPreprocessingTrainer(new MinMaxScalerTrainer<Integer, Vector>())
                    .addPreprocessingTrainer(new NormalizationTrainer<Integer, Vector>().withP(1))
                    .addPreprocessingTrainer(getCustomTrainer())
                    .addTrainer(new DecisionTreeClassificationTrainer(5, 0))
                    .fit(ignite, trainingSet);

                System.out.println(">>> Perform scoring.");
                double score = Evaluator.evaluate(trainingSet,
                    mdl, mdl.getPreprocessor(), MetricName.R2
                );

                System.out.println(">>> R^2 score: " + score);
            }
            finally {
                if (trainingSet != null)
                    trainingSet.destroy();
            }
        }
        finally {
            System.out.flush();
        }
    }

    /**
     * Custom trainer for preprocessor. This trainer just returns custom preprocessor based on given preprocessor. Such
     * trainers could be used for injecting custom preprocessors to Pipelines.
     *
     * @return Trainer for preprocessor that normalizes given vectors.
     */
    private static PreprocessingTrainer<Integer, Vector> getCustomTrainer() {
        return new PreprocessingTrainer<Integer, Vector>() {
            @Override public Preprocessor<Integer, Vector> fit(LearningEnvironmentBuilder envBuilder,
                DatasetBuilder<Integer, Vector> datasetBuilder,
                Preprocessor<Integer, Vector> basePreprocessor) {
                return (k, v) -> {
                    LabeledVector res = basePreprocessor.apply(k, v);
                    return res.features().normalize().labeled(res.label());
                };
            }
        };
    }

    /**
     * Custom vectorizer based on preprocessor. This vectorizer extends given vectors by values equal to log(feature)
     * for each feature in vector.
     */
    private static class Vectorizer9000 extends Vectorizer.VectorizerAdapter<Integer, Vector, Integer, Double> {
        /**
         * Base preprocessor.
         */
        private final Preprocessor<Integer, Vector> basePreprocessor;

        /**
         * Creates an instance of vectorizer.
         *
         * @param basePreprocessor Base preprocessor.
         */
        public Vectorizer9000(Preprocessor<Integer, Vector> basePreprocessor) {
            this.basePreprocessor = basePreprocessor;
        }

        /**
         * {@inheritDoc}
         */
        @Override public LabeledVector<Double> apply(Integer key, Vector val) {
            LabeledVector<Double> baseVec = basePreprocessor.apply(key, val);
            double[] features = new double[baseVec.size() * 2];
            for (int i = 0; i < baseVec.size(); i++) {
                features[i] = baseVec.get(i);
                double logVal = Math.log(baseVec.get(i));
                features[i + baseVec.size()] = Double.isInfinite(logVal) || Double.isNaN(logVal) ? -1. : logVal;
            }

            return VectorUtils.of(features).labeled(baseVec.label());
        }
    }
}
