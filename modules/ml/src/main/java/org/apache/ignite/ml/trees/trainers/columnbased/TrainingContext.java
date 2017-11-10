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

package org.apache.ignite.ml.trees.trainers.columnbased;

import com.zaxxer.sparsebits.SparseBitSet;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.trees.ContinuousRegionInfo;
import org.apache.ignite.ml.trees.ContinuousSplitCalculator;
import org.apache.ignite.ml.trees.RegionInfo;
import org.apache.ignite.ml.trees.trainers.columnbased.caches.FeaturesCache;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.CategoricalFeatureProcessor;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.ContinuousFeatureProcessor;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.FeatureProcessor;

import java.util.Map;
import java.util.UUID;
import java.util.stream.DoubleStream;

import static org.apache.ignite.ml.trees.trainers.columnbased.caches.FeaturesCache.COLUMN_DECISION_TREE_TRAINER_FEATURES_CACHE_NAME;

/**
 * Context of training with {@link ColumnDecisionTreeTrainer}.
 *
 * @param <D> Class for storing of information used in calculation of impurity of continuous feature region.
 */
public class TrainingContext<D extends ContinuousRegionInfo> {
    /** Input for training with {@link ColumnDecisionTreeTrainer}. */
    private final ColumnDecisionTreeTrainerInput input;

    /** Labels. */
    private final double[] labels;

    /** Calculator used for finding splits of region of continuous features. */
    private final ContinuousSplitCalculator<D> continuousSplitCalculator;

    /** Calculator used for finding splits of region of categorical feature. */
    private final IgniteFunction<DoubleStream, Double> categoricalSplitCalculator;

    /** UUID of current training. */
    private final UUID trainingUUID;

    /**
     * Construct context for training with {@link ColumnDecisionTreeTrainer}.
     *
     * @param input Input for training.
     * @param continuousSplitCalculator Calculator used for calculations of splits of continuous features regions.
     * @param categoricalSplitCalculator Calculator used for calculations of splits of categorical features regions.
     * @param trainingUUID UUID of the current training.
     * @param ignite Ignite instance.
     */
    public TrainingContext(ColumnDecisionTreeTrainerInput input,
        ContinuousSplitCalculator<D> continuousSplitCalculator,
        IgniteFunction<DoubleStream, Double> categoricalSplitCalculator,
        UUID trainingUUID,
        Ignite ignite) {
        this.input = input;
        this.labels = input.labels(ignite);
        this.continuousSplitCalculator = continuousSplitCalculator;
        this.categoricalSplitCalculator = categoricalSplitCalculator;
        this.trainingUUID = trainingUUID;
    }

    /**
     * Get processor used for calculating splits of categorical features.
     *
     * @param catsCnt Count of categories.
     * @return Processor used for calculating splits of categorical features.
     */
    public CategoricalFeatureProcessor categoricalFeatureProcessor(int catsCnt) {
        return new CategoricalFeatureProcessor(categoricalSplitCalculator, catsCnt);
    }

    /**
     * Get processor used for calculating splits of continuous features.
     *
     * @return Processor used for calculating splits of continuous features.
     */
    public ContinuousFeatureProcessor<D> continuousFeatureProcessor() {
        return new ContinuousFeatureProcessor<>(continuousSplitCalculator);
    }

    /**
     * Get labels.
     *
     * @return Labels.
     */
    public double[] labels() {
        return labels;
    }

    /**
     * Get values of feature with given index.
     *
     * @param featIdx Feature index.
     * @param ignite Ignite instance.
     * @return Values of feature with given index.
     */
    public double[] values(int featIdx, Ignite ignite) {
        IgniteCache<FeaturesCache.FeatureKey, double[]> featuresCache = ignite.getOrCreateCache(COLUMN_DECISION_TREE_TRAINER_FEATURES_CACHE_NAME);
        return featuresCache.localPeek(FeaturesCache.getFeatureCacheKey(featIdx, trainingUUID, input.affinityKey(featIdx, ignite)));
    }

    /**
     * Perform best split on the given region projection.
     *
     * @param input Input of {@link ColumnDecisionTreeTrainer} performing split.
     * @param bitSet Bit set specifying split.
     * @param targetFeatIdx Index of feature for performing split.
     * @param bestFeatIdx Index of feature with best split.
     * @param targetRegionPrj Projection of region to split on feature with index {@code featureIdx}.
     * @param leftData Data of left region of split.
     * @param rightData Data of right region of split.
     * @param ignite Ignite instance.
     * @return Perform best split on the given region projection.
     */
    public IgniteBiTuple<RegionProjection, RegionProjection> performSplit(ColumnDecisionTreeTrainerInput input,
        SparseBitSet bitSet, int targetFeatIdx, int bestFeatIdx, RegionProjection targetRegionPrj, RegionInfo leftData,
        RegionInfo rightData, Ignite ignite) {

        Map<Integer, Integer> catFeaturesInfo = input.catFeaturesInfo();

        if (!catFeaturesInfo.containsKey(targetFeatIdx) && !catFeaturesInfo.containsKey(bestFeatIdx))
            return continuousFeatureProcessor().performSplit(bitSet, targetRegionPrj, (D)leftData, (D)rightData);
        else if (catFeaturesInfo.containsKey(targetFeatIdx))
            return categoricalFeatureProcessor(catFeaturesInfo.get(targetFeatIdx)).performSplitGeneric(bitSet, values(targetFeatIdx, ignite), targetRegionPrj, leftData, rightData);
        return continuousFeatureProcessor().performSplitGeneric(bitSet, labels, targetRegionPrj, leftData, rightData);
    }

    /**
     * Processor used for calculating splits for feature with the given index.
     *
     * @param featureIdx Index of feature to process.
     * @return Processor used for calculating splits for feature with the given index.
     */
    public FeatureProcessor featureProcessor(int featureIdx) {
        return input.catFeaturesInfo().containsKey(featureIdx) ? categoricalFeatureProcessor(input.catFeaturesInfo().get(featureIdx)) : continuousFeatureProcessor();
    }

    /**
     * Shortcut for affinity key.
     *
     * @param idx Feature index.
     * @return Affinity key.
     */
    public Object affinityKey(int idx) {
        return input.affinityKey(idx, Ignition.localIgnite());
    }
}
