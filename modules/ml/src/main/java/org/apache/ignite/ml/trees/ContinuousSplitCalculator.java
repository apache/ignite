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

import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.SampleInfo;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.SplitInfo;

/**
 * This class is used for calculation of best split by continuous feature.
 * @param <C> Class in which information about region will be stored.
 */
public interface ContinuousSplitCalculator<C extends ContinuousRegionInfo> {
    /**
     * Calculate region info 'from scratch'.
     * @param s Stream of labels in this region.
     * @param l Index of sample projection on this feature in array sorted by this projection value and intervals bitsets.
     *      ({@see org.apache.ignite.ml.trees.trainers.columnbased.vectors.ContinuousFeatureVector}).
     * @return Region info.
     */
    C calculateRegionInfo(DoubleStream s, int l);

    /**
     * Calculate split info of best split of region given information about this region.
     * @param s Stream of labels in this region.
     * @param regionIdx Index of region being split.
     * @param data Information about region being split which can be used for computations.
     * @return Information about best split of region with index given by regionIdx.
     */
    SplitInfo<C> splitRegion(Stream<SampleInfo> s, int regionIdx, C data);
}
