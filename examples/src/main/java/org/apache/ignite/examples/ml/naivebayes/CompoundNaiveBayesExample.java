package org.apache.ignite.examples.ml.naivebayes;

import java.io.FileNotFoundException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.naivebayes.compound.CompoundNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.compound.CompoundNaiveBayesTrainer;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesTrainer;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.BinaryClassificationEvaluator;
import org.apache.ignite.ml.util.MLSandboxDatasets;
import org.apache.ignite.ml.util.SandboxMLCache;

import static java.util.Arrays.asList;

public class CompoundNaiveBayesExample {
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(">>> Compound Naive Bayes classification model over partitioned dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = new SandboxMLCache(ignite)
                .fillCacheWith(MLSandboxDatasets.MIXED_DATASET);

            double[] classProbabilities = new double[] {.5, .5};
            double[][] thresholds = new double[][] {{.5}, {.5}, {.5}, {.5}, {.5}};

            System.out.println(">>> Create new naive Bayes classification trainer object.");
            CompoundNaiveBayesTrainer trainer = new CompoundNaiveBayesTrainer()
                .setClsProbabilities(classProbabilities)
                .setGaussianNaiveBayesTrainer(new GaussianNaiveBayesTrainer()
                    .setFeatureIdsToSkip(asList(3, 4, 5, 6, 7)))
                .setDiscreteNaiveBayesTrainer(new DiscreteNaiveBayesTrainer()
                    .setBucketThresholds(thresholds)
                    .setFeatureIdsToSkip(asList(0, 1, 2)));
            System.out.println(">>> Perform the training to get the model.");

            IgniteBiFunction<Integer, Vector, Vector> featureExtractor = (k, v) -> v.copyOfRange(1, v.size());
            IgniteBiFunction<Integer, Vector, Double> lbExtractor = (k, v) -> v.get(0);

            CompoundNaiveBayesModel mdl = trainer.fit(
                ignite,
                dataCache,
                featureExtractor,
                lbExtractor
            );

            System.out.println(">>> Compound Naive Bayes model: " + mdl);

            double accuracy = BinaryClassificationEvaluator.evaluate(
                dataCache,
                mdl,
                featureExtractor,
                lbExtractor
            ).accuracy();

            System.out.println("\n>>> Accuracy " + accuracy);

            System.out.println(">>> Compound Naive bayes model over partitioned dataset usage example completed.");
        }
    }
}
