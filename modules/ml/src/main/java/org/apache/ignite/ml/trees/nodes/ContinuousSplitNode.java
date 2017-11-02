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

package org.apache.ignite.ml.trees.nodes;

import org.apache.ignite.ml.math.Vector;

/**
 * Split node representing split of continuous feature.
 */
public class ContinuousSplitNode extends SplitNode {
    /** Threshold. Values which are less or equal then threshold are assigned to the left subregion. */
    private double threshold;

    /**
     * Construct ContinuousSplitNode by threshold and feature index.
     *
     * @param threshold Threshold.
     * @param featureIdx Feature index.
     */
    public ContinuousSplitNode(double threshold, int featureIdx) {
        super(featureIdx);
        this.threshold = threshold;
    }

    /** {@inheritDoc} */
    @Override public boolean goLeft(Vector v) {
        return v.getX(featureIdx) <= threshold;
    }

    /** Threshold. Values which are less or equal then threshold are assigned to the left subregion. */
    public double threshold() {
        return threshold;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ContinuousSplitNode [" +
            "threshold=" + threshold +
            ']';
    }
}
