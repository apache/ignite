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

import org.apache.ignite.ml.trees.RegionInfo;
import org.apache.ignite.ml.trees.nodes.SplitNode;

/**
 * Class encapsulating information about the split.
 *
 * @param <D> Class representing information of left and right subregions.
 */
public abstract class SplitInfo<D extends RegionInfo> {
    /** Information gain of this split. */
    protected double infoGain;

    /** Index of the region to split. */
    protected final int regionIdx;

    /** Data of left subregion. */
    protected final D leftData;

    /** Data of right subregion. */
    protected final D rightData;

    /**
     * Construct the split info.
     *
     * @param regionIdx Index of the region to split.
     * @param leftData Data of left subregion.
     * @param rightData Data of right subregion.
     */
    public SplitInfo(int regionIdx, D leftData, D rightData) {
        this.regionIdx = regionIdx;
        this.leftData = leftData;
        this.rightData = rightData;
    }

    /**
     * Index of region to split.
     *
     * @return Index of region to split.
     */
    public int regionIndex() {
        return regionIdx;
    }

    /**
     * Information gain of the split.
     *
     * @return Information gain of the split.
     */
    public double infoGain() {
        return infoGain;
    }

    /**
     * Data of right subregion.
     *
     * @return Data of right subregion.
     */
    public D rightData() {
        return rightData;
    }

    /**
     * Data of left subregion.
     *
     * @return Data of left subregion.
     */
    public D leftData() {
        return leftData;
    }

    /**
     * Create SplitNode from this split info.
     *
     * @param featureIdx Index of feature by which goes split.
     * @return SplitNode from this split info.
     */
    public abstract SplitNode createSplitNode(int featureIdx);

    /**
     * Set information gain.
     *
     * @param infoGain Information gain.
     */
    public void setInfoGain(double infoGain) {
        this.infoGain = infoGain;
    }
}
