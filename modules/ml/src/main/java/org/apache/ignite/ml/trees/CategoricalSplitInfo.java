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

import org.apache.ignite.ml.trees.nodes.CategoricalSplitNode;
import org.apache.ignite.ml.trees.nodes.SplitNode;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.SplitInfo;

import java.util.BitSet;

/**
 * Information about split of categorical feature.
 *
 * @param <D> Class representing information of left and right subregions.
 */
public class CategoricalSplitInfo<D extends RegionInfo> extends SplitInfo<D> {
    /** Bitset indicating which vectors are assigned to left subregion. */
    private final BitSet bs;

    /**
     * @param regionIdx Index of region which is split.
     * @param leftData Data of left subregion.
     * @param rightData Data of right subregion.
     * @param bs Bitset indicating which vectors are assigned to left subregion.
     */
    public CategoricalSplitInfo(int regionIdx, D leftData, D rightData,
        BitSet bs) {
        super(regionIdx, leftData, rightData);
        this.bs = bs;
    }

    /** {@inheritDoc} */
    @Override public SplitNode createSplitNode(int featureIdx) {
        return new CategoricalSplitNode(featureIdx, bs);
    }

    /**
     * Get bitset indicating which vectors are assigned to left subregion.
     */
    public BitSet bitSet() {
        return bs;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CategoricalSplitInfo [" +
            "infoGain=" + infoGain +
            ", regionIdx=" + regionIdx +
            ", leftData=" + leftData +
            ", bs=" + bs +
            ", rightData=" + rightData +
            ']';
    }
}
