package org.apache.ignite.ml.knn.classification;

import java.util.TreeMap;

/**
 * Created by zaleslaw on 09.08.18.
 */
public class ProbableLabel {
    /** Key is label, value is probability to be this class */
    TreeMap<Double, Double> clsLbls;

    public ProbableLabel(TreeMap<Double, Double> clsLbls) {
        this.clsLbls = clsLbls;
    }
}
