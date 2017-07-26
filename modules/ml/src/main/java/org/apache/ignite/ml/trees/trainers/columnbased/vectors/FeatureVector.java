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

import com.zaxxer.sparsebits.SparseBitSet;
import java.util.stream.DoubleStream;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.trees.RegionInfo;

/**
 * Base interface for feature vectors used in {@see org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer}
 *
 * @param <D> Class representing data of regions resulted from split.
 * @param <S> Class representing data of split.
 */
public interface FeatureVector<D extends RegionInfo, S extends SplitInfo<D>> {
    /**
     * Finds best split by this feature among all splits of all regions.
     *
     * @return best split by this feature among all splits of all regions.
     */
    SplitInfo findBestSplit();

    /**
     * Calculates the bitset mapping each data point to left (corresponding bit is set) or right subregion.
     *
     * @param s data used for calculating the split.
     * @return Bitset mapping each data point to left (corresponding bit is set) or right subregion.
     */
    SparseBitSet calculateOwnershipBitSet(S s);

    /**
     * Splits given region using bitset which maps data point to left or right subregion.
     * This method is present for the vectors of the same type to be able to pass between them information about regions
     * and therefore used iff the optimal split is received on feature of the same type.
     *
     * @param bs Bitset which maps data point to left or right subregion.
     * @param regionIdx Index of region to split.
     * @param leftData Data of the left subregion.
     * @param rightData Data of the right subregion.
     * @return This feature vector.
     */
    FeatureVector<D, S> performSplit(SparseBitSet bs, int regionIdx, D leftData, D rightData);

    /**
     * Splits given region using bitset which maps data point to left or right subregion. This method is used iff the
     * optimal split is received on feature of different type, therefore information about regions is limited to the
     * {@see RegionInfo} class which is base for all classes used to represent region data.
     *
     * @param bs Bitset which maps data point to left or right subregion.
     * @param regionIdx Index of region to split.
     * @param leftData Data of the left subregion.
     * @param rightData Data of the right subregion.
     * @return This feature vector.
     */
    FeatureVector<D, S> performSplitGeneric(SparseBitSet bs, int regionIdx, RegionInfo leftData, RegionInfo rightData);

    /**
     * Calculates values of all regions with a given function.
     *
     * @param regCalc Function used for calculation value of any region.
     * @return Values in regions.
     */
    double[] calculateRegions(IgniteFunction<DoubleStream, Double> regCalc);
}
