package org.apache.ignite.ml.naivebayes.gaussian;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.util.MapUtil;

/** /** Service class is used to calculate means and vaiances */
class GaussianNaiveBayesSumsHolder implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 1L;
    /** Sum of all values for all features for each label */
    Map<Double, double[]> featureSumsPerLbl = new HashMap<>();
    /** Sum of all squared values for all features for each label */
    Map<Double, double[]> featureSquaredSumsPerLbl = new HashMap<>();
    /** Rows count for each label */
    Map<Double, Integer> featureCountersPerLbl = new HashMap<>();

    /** Merge current */
    GaussianNaiveBayesSumsHolder merge(GaussianNaiveBayesSumsHolder other) {
        featureSumsPerLbl = MapUtil.mergeMaps(featureSumsPerLbl, other.featureSumsPerLbl, this::sum, HashMap::new);
        featureSquaredSumsPerLbl = MapUtil.mergeMaps(featureSquaredSumsPerLbl, other.featureSquaredSumsPerLbl, this::sum, HashMap::new);
        featureCountersPerLbl = MapUtil.mergeMaps(featureCountersPerLbl, other.featureCountersPerLbl, (i1, i2) -> i1 + i2, HashMap::new);
        return this;
    }

    private double[] sum(double[] arr1, double[] arr2) {
        for (int i = 0; i < arr1.length; i++) {
            arr1[i] += arr2[i];
        }
        return arr1;
    }
}
