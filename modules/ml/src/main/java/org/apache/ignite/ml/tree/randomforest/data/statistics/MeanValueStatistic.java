/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.tree.randomforest.data.statistics;

import java.io.Serializable;

/**
 * Statistics for mean value computing container.
 */
public class MeanValueStatistic implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = -6265792209142062174L;

    /** Sum of values. */
    private double sumOfValues;

    /** Count of values. */
    private long cntOfValues;

    /**
     * Creates an instance of MeanValueStatistic.
     *
     * @param sumOfValues Sum of values.
     * @param cntOfValues Count of values.
     */
    public MeanValueStatistic(double sumOfValues, long cntOfValues) {
        this.sumOfValues = sumOfValues;
        this.cntOfValues = cntOfValues;
    }

    /**
     * @return Mean value.
     */
    public double mean() {
        return sumOfValues / cntOfValues;
    }

    /** */
    public double getSumOfValues() {
        return sumOfValues;
    }

    /** */
    public void setSumOfValues(double sumOfValues) {
        this.sumOfValues = sumOfValues;
    }

    /** */
    public long getCntOfValues() {
        return cntOfValues;
    }

    /** */
    public void setCntOfValues(long cntOfValues) {
        this.cntOfValues = cntOfValues;
    }
}
