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

package org.apache.ignite.ml.tree.randomforest.data;

import java.util.List;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Class contains a default implementations of some features count selection strategies for random forest.
 */
public class FeaturesCountSelectionStrategies {
    /** */
    public static IgniteFunction<List<FeatureMeta>, Integer> SQRT = (List<FeatureMeta> meta) -> {
        return (int)Math.sqrt(meta.size());
    };

    /** */
    public static IgniteFunction<List<FeatureMeta>, Integer> ALL = (List<FeatureMeta> meta) -> {
        return meta.size();
    };

    /** */
    public static IgniteFunction<List<FeatureMeta>, Integer> LOG2 = (List<FeatureMeta> meta) -> {
        return (int)(Math.log(meta.size()) / Math.log(2));
    };

    /** */
    public static IgniteFunction<List<FeatureMeta>, Integer> ONE_THIRD = (List<FeatureMeta> meta) -> {
        return (int)(meta.size() / 3);
    };
}
