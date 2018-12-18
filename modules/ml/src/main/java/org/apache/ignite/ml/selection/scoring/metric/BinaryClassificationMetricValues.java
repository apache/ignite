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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides access
 */
public class BinaryClassificationMetricValues {
    /** True Positive (TP). */
    private double tp;

    /** True Negative (TN). */
    private double tn;

    /** False Positive (FP). */
    private double fp;

    /** False Negative (FN). */
    private double fn;

    /** Sensitivity or True Positive Rate (TPR). */
    private double recall;

    /** Specificity (SPC) or True Negative Rate (TNR). */
    private double specificity;

    /** Precision or Positive Predictive Value (PPV). */
    private double precision;

    /** Negative Predictive Value (NPV). */
    private double npv;

    /** Fall-out or False Positive Rate (FPR). */
    private double fallOut;

    /** False Discovery Rate (FDR). */
    private double fdr;

    /** Miss Rate or False Negative Rate (FNR). */
    private double missRate;

    /** Accuracy. */
    private double accuracy;

    /** Balanced accuracy. */
    private double balancedAccuracy;

    /** F1-Score is the harmonic mean of Precision and Sensitivity. */
    private double f1Score;

    /** */
    public double tp() {
        return tp;
    }

    /** */
    public void setTp(double tp) {
        this.tp = tp;
    }

    /** */
    public double tn() {
        return tn;
    }

    /** */
    public void setTn(double tn) {
        this.tn = tn;
    }

    /** */
    public double fp() {
        return fp;
    }

    /** */
    public void setFp(double fp) {
        this.fp = fp;
    }

    /** */
    public double fn() {
        return fn;
    }

    /** */
    public void setFn(double fn) {
        this.fn = fn;
    }

    /** */
    public double recall() {
        return recall;
    }

    /** */
    public void setRecall(double recall) {
        this.recall = recall;
    }

    /** */
    public double specificity() {
        return specificity;
    }

    /** */
    public void setSpecificity(double specificity) {
        this.specificity = specificity;
    }

    /** */
    public double precision() {
        return precision;
    }

    /** */
    public void setPrecision(double precision) {
        this.precision = precision;
    }

    /** */
    public double npv() {
        return npv;
    }

    /** */
    public void setNpv(double npv) {
        this.npv = npv;
    }

    /** */
    public double fallOut() {
        return fallOut;
    }

    /** */
    public void setFallOut(double fallOut) {
        this.fallOut = fallOut;
    }

    /** */
    public double fdr() {
        return fdr;
    }

    /** */
    public void setFdr(double fdr) {
        this.fdr = fdr;
    }

    /** */
    public double missRate() {
        return missRate;
    }

    /** */
    public void setMissRate(double missRate) {
        this.missRate = missRate;
    }

    /** */
    public double accuracy() {
        return accuracy;
    }

    /** */
    public void setAccuracy(double accuracy) {
        this.accuracy = accuracy;
    }

    /** */
    public double balancedAccuracy() {
        return balancedAccuracy;
    }

    /** */
    public void setBalancedAccuracy(double balancedAccuracy) {
        this.balancedAccuracy = balancedAccuracy;
    }

    /** */
    public double f1Score() {
        return f1Score;
    }

    /** */
    public void setF1Score(double f1Score) {
        this.f1Score = f1Score;
    }

    /** Returns the pair of metric name and metric value. */
    public Map<String, Double> toMap() {
        Map<String, Double> metricValues = new HashMap<>();
        for (Field field : this.getClass().getDeclaredFields())
            try {
                metricValues.put(field.getName(), field.getDouble(this));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        return metricValues;
    }
}
