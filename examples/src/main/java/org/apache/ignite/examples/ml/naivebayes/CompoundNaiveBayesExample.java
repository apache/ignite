package org.apache.ignite.examples.ml.naivebayes;

import java.io.FileNotFoundException;
import java.util.Vector;

public class CompoundNaiveBayesExample {
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(">>> Compound Naive Bayes classification model over partitioned dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = new SandboxMLCache(ignite)
                    .fillCacheWith(MLSandboxDatasets.TWO_CLASSED_IRIS);

            System.out.println(">>> Create new naive Bayes classification trainer object.");
            CompoundNaiveBayesTrainer trainer = new CompoundNaiveBayesTrainer();

            System.out.println(">>> Perform the training to get the model.");

            IgniteBiFunction<Integer, Vector, Vector> featureExtractor = (k, v) -> v.copyOfRange(1, v.size());
            IgniteBiFunction<Integer, Vector, Double> lbExtractor = (k, v) -> v.get(0);

            CompoundNaiveBayesModel mdl = trainer.fit(
                    ignite,
                    dataCache,
                    featureExtractor,
                    lbExtractor
            );

            System.out.println(">>> Naive Bayes model: " + mdl);

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
