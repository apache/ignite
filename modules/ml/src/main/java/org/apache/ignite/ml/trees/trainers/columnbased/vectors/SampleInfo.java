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

package org.apache.ignite.ml.trees.trainers.columnbased.vectors;

/**
 * Information about given sample within given fixed feature.
 */
public class SampleInfo {
    /** Label of this sample. */
    private double lb;

    /** Value of projection of this sample on given fixed feature. */
    private double val;

    /** Sample index. */
    private int sampleInd;

    /**
     * @param lb Label of this sample.
     * @param val Value of projection of this sample on given fixed feature.
     * @param sampleInd Sample index.
     */
    public SampleInfo(double lb, double val, int sampleInd) {
        this.lb = lb;
        this.val = val;
        this.sampleInd = sampleInd;
    }

    /**
     * Get the label of this sample.
     *
     * @return Label of this sample.
     */
    public double getLabel() {
        return lb;
    }

    /**
     * Get the value of projection of this sample on given fixed feature.
     *
     * @return Value of projection of this sample on given fixed feature.
     */
    public double getVal() {
        return val;
    }

    /**
     * Get the sample index.
     *
     * @return Sample index.
     */
    public int getSampleInd() {
        return sampleInd;
    }
}
