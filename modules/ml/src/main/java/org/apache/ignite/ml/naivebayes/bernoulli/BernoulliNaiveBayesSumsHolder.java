package org.apache.ignite.ml.naivebayes.bernoulli;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.util.MapUtil;

/** Created by Ravil on 26/10/2018. */
public class BernoulliNaiveBayesSumsHolder implements AutoCloseable, Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 1L;
    /** Sum of all values for all features for each label */
    Map<Double, long[]> onesCountPerLbl = new HashMap<>();
    /** Rows count for each label */
    Map<Double, Integer> featureCountersPerLbl = new HashMap<>();

    /** Merge to current */
    BernoulliNaiveBayesSumsHolder merge(BernoulliNaiveBayesSumsHolder other) {
        onesCountPerLbl = MapUtil.mergeMaps(onesCountPerLbl, other.onesCountPerLbl, this::sum, HashMap::new);
        featureCountersPerLbl = MapUtil.mergeMaps(featureCountersPerLbl, other.featureCountersPerLbl, (i1, i2) -> i1 + i2, HashMap::new);
        return this;
    }

    /** In-place operation. Sums {@code arr2} to {@code arr1} element to element. */
    private long[] sum(long[] arr1, long[] arr2) {
        for (int i = 0; i < arr1.length; i++) {
            arr1[i] += arr2[i];
        }
        return arr1;
    }

    /** */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
