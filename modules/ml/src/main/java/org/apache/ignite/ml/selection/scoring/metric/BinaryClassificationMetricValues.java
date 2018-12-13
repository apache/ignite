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

public class BinaryClassificationMetricValues {
    private double tp;
    private double tn;
    private double fp;
    private double fn;
    private double recall;
    private double specificity;
    private double precision;
    private double npv;
    private double fallOut;
    private double fdr;
    private double missRate;
    private double accuracy;
    private double balancedAccuracy;
    private double f1Score;

    public double tp() {
        return tp;
    }

    public void setTp(double tp) {
        this.tp = tp;
    }

    public double tn() {
        return tn;
    }

    public void setTn(double tn) {
        this.tn = tn;
    }

    public double fp() {
        return fp;
    }

    public void setFp(double fp) {
        this.fp = fp;
    }

    public double fn() {
        return fn;
    }

    public void setFn(double fn) {
        this.fn = fn;
    }

    public double recall() {
        return recall;
    }

    public void setRecall(double recall) {
        this.recall = recall;
    }

    public double specificity() {
        return specificity;
    }

    public void setSpecificity(double specificity) {
        this.specificity = specificity;
    }

    public double precision() {
        return precision;
    }

    public void setPrecision(double precision) {
        this.precision = precision;
    }

    public double npv() {
        return npv;
    }

    public void setNpv(double npv) {
        this.npv = npv;
    }

    public double fallOut() {
        return fallOut;
    }

    public void setFallOut(double fallOut) {
        this.fallOut = fallOut;
    }

    public double fdr() {
        return fdr;
    }

    public void setFdr(double fdr) {
        this.fdr = fdr;
    }

    public double missRate() {
        return missRate;
    }

    public void setMissRate(double missRate) {
        this.missRate = missRate;
    }

    public double accuracy() {
        return accuracy;
    }

    public void setAccuracy(double accuracy) {
        this.accuracy = accuracy;
    }

    public double balancedAccuracy() {
        return balancedAccuracy;
    }

    public void setBalancedAccuracy(double balancedAccuracy) {
        this.balancedAccuracy = balancedAccuracy;
    }

    public double f1Score() {
        return f1Score;
    }

    public void setF1Score(double f1Score) {
        this.f1Score = f1Score;
    }
}
