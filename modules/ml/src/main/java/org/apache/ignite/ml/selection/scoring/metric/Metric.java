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

package org.apache.ignite.ml.selection.scoring.metric;

import java.util.Iterator;
import org.apache.ignite.ml.selection.scoring.LabelPair;

/**
 * Base interface for score calculators.
 *
 * @param <L> Type of a label (truth or prediction).
 */
public interface Metric<L> {
    /**
     * Calculates score.
     *
     * @param iter Iterator that supplies pairs of truth values and predicated.
     * @return Score.
     */
    public double score(Iterator<LabelPair<L>> iter);

    /**
     * Returns the metric's name.
     *
     * NOTE: Should be unique to calculate multiple metrics correctly.
     *
     * @return String name representation.
     */
    public String name();
}
