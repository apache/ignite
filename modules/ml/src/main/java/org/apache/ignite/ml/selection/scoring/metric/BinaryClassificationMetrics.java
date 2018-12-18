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

package org.apache.ignite.ml.selection.scoring.metric;

import java.util.Iterator;
import org.apache.ignite.ml.selection.scoring.LabelPair;

/**
 * Binary classification metrics calculator.
 */
public class BinaryClassificationMetrics {
    /** Positive class label. */
    private double positiveClsLb = 1.0;

    /** Negative class label. */
    private double negativeClsLb = 0.0;

    /**
     * Calculates binary metrics values.
     *
     * @param iter Iterator that supplies pairs of truth values and predicated.
     * @return Scores for all binary metrics.
     */
    public BinaryClassificationMetricValues score(Iterator<LabelPair<Double>> iter) {
        BinaryClassificationMetricValues metricValues = new BinaryClassificationMetricValues();

        long tp = 0;
        long tn = 0;
        long fp = 0;
        long fn = 0;

        while (iter.hasNext()) {
            LabelPair<Double> e = iter.next();

            double prediction = e.getPrediction();
            double truth = e.getTruth();

            if (prediction != negativeClsLb && prediction != positiveClsLb)
                throw new UnknownClassLabelException(prediction, positiveClsLb, negativeClsLb);
            if (truth != negativeClsLb && truth != positiveClsLb)
                throw new UnknownClassLabelException(truth, positiveClsLb, negativeClsLb);

            if (truth == positiveClsLb && prediction == positiveClsLb) tp++;
            else if (truth == positiveClsLb && prediction == negativeClsLb) fn++;
            else if (truth == negativeClsLb && prediction == negativeClsLb) tn++;
            else if (truth == negativeClsLb && prediction == positiveClsLb) fp++;
        }

        long p = tp + fn;
        long n = tn + fp;
        long positivePredictions = tp + fp;
        long negativePredictions = tn + fn;

        // according to https://github.com/dice-group/gerbil/wiki/Precision,-Recall-and-F1-measure
        double recall = p == 0 ? 1 : (double) tp / p;
        double precision = positivePredictions == 0 ? 1 : (double) tp / positivePredictions;
        double specificity = n == 0 ? 1 : (double) tn / n;
        double npv = negativePredictions == 0 ? 1 : (double) tn / negativePredictions;
        double fallOut = n == 0 ? 1 : (double) fp / n;
        double fdr = positivePredictions == 0 ? 1 : (double) fp / positivePredictions;
        double missRate = p == 0 ? 1 : (double) fn / p;

        double f1Score = 2 * (recall * precision) / (recall + precision);

        double accuracy = (p + n) == 0 ? 1 : (double) (tp + tn) / (p + n); // multiplication on 1.0 to make double
        double balancedAccuracy = p == 0 && n == 0 ? 1 : ((double) tp / p + (double) tn / n) / 2;

        metricValues.setAccuracy(accuracy);
        metricValues.setBalancedAccuracy(balancedAccuracy);
        metricValues.setF1Score(f1Score);
        metricValues.setFallOut(fallOut);
        metricValues.setFdr(fdr);
        metricValues.setFn(fn);
        metricValues.setFp(fp);
        metricValues.setMissRate(missRate);
        metricValues.setNpv(npv);
        metricValues.setPrecision(precision);
        metricValues.setRecall(recall);
        metricValues.setSpecificity(specificity);
        metricValues.setTn(tn);
        metricValues.setTp(tp);

        return metricValues;
    }

    /** */
    public double positiveClsLb() {
        return positiveClsLb;
    }

    /** */
    public BinaryClassificationMetrics withPositiveClsLb(double positiveClsLb) {
        this.positiveClsLb = positiveClsLb;
        return this;
    }

    /** */
    public double negativeClsLb() {
        return negativeClsLb;
    }

    /** */
    public BinaryClassificationMetrics withNegativeClsLb(double negativeClsLb) {
        this.negativeClsLb = negativeClsLb;
        return this;
    }
}
