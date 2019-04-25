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

package org.apache.ignite.ml.tree.leaf;

import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.tree.DecisionTreeLeafNode;
import org.apache.ignite.ml.tree.TreeFilter;
import org.apache.ignite.ml.tree.data.DecisionTreeData;

/**
 * Base interface for decision tree leaf builders.
 */
public interface DecisionTreeLeafBuilder {
    /**
     * Creates new leaf node for given dataset and node predicate.
     *
     * @param dataset Dataset.
     * @param pred Node predicate.
     * @return Leaf node.
     */
    public DecisionTreeLeafNode createLeafNode(Dataset<EmptyContext, DecisionTreeData> dataset, TreeFilter pred);
}
