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

package org.apache.ignite.ml.trees;

/**
 * Information about region used by continuous features.
 */
public class ContinuousRegionInfo extends RegionInfo {
    /**
     * Left bound of this region denoted by index in a sorted array of samples (for sorting order {@see
     * org.apache.ignite.ml.trees.trainers.columnbased.vectors.ContinuousFeatureVector}).
     */
    protected int left;

    /**
     * Right bound of this region denoted by index in a sorted array of samples (for sorting order {@see
     * org.apache.ignite.ml.trees.trainers.columnbased.vectors.ContinuousFeatureVector}).
     */
    protected int right;

    /**
     * @param impurity Impurity of the region.
     * @param left Left bound of this region denoted by index in a sorted array of samples (for sorting order {@see
     * org.apache.ignite.ml.trees.trainers.columnbased.vectors.ContinuousFeatureVector}).
     * @param right Right bound of this region denoted by index in a sorted array of samples (for sorting order {@see
     * org.apache.ignite.ml.trees.trainers.columnbased.vectors.ContinuousFeatureVector}).
     */
    public ContinuousRegionInfo(double impurity, int left, int right) {
        super(impurity);
        this.left = left;
        this.right = right;
    }

    /**
     * Left bound of this region denoted by index in a sorted array of samples (for sorting order {@see
     * org.apache.ignite.ml.trees.trainers.columnbased.vectors.ContinuousFeatureVector}).
     */
    public int left() {
        return left;
    }

    /**
     * Right bound of this region denoted by index in a sorted array of samples (for sorting order {@see
     * org.apache.ignite.ml.trees.trainers.columnbased.vectors.ContinuousFeatureVector}).
     */
    public int right() {
        return right;
    }
}