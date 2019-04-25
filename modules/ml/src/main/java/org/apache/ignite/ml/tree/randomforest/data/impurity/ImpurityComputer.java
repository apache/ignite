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

package org.apache.ignite.ml.tree.randomforest.data.impurity;

import java.util.Optional;
import org.apache.ignite.ml.dataset.feature.Histogram;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;

/**
 * Interface represents an object that can compute best splitting point using features histograms.
 *
 * @param <T> Base object type for histogram.
 * @param <H> Type of histogram that can be used in math operations with this object.
 */
public interface ImpurityComputer<T, H extends Histogram<T, H>> extends Histogram<T, H> {
    /**
     * Returns best split point computed on histogram if it exists.
     * Split point may be absent when there is no data in histograms or split point lay in last bucket in histogram.
     *
     * @return Splitting point for decision tree.
     */
    public Optional<NodeSplit> findBestSplit();
}
