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

package org.apache.ignite.ml.tree.randomforest;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import org.apache.ignite.ml.tree.randomforest.data.TreeNode;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** */
public class RandomForestTest {
    /** Seed. */
    private final long seed = 0;

    /** Count of trees. */
    private final int cntOfTrees = 10;

    /** Min imp delta. */
    private final double minImpDelta = 1.0;

    /** Max depth. */
    private final int maxDepth = 1;

    /** Meta. */
    private final List<FeatureMeta> meta = Arrays.asList(
        new FeatureMeta("", 0, false),
        new FeatureMeta("", 1, true),
        new FeatureMeta("", 2, false),
        new FeatureMeta("", 3, true),
        new FeatureMeta("", 4, false),
        new FeatureMeta("", 5, true),
        new FeatureMeta("", 6, false)
    );

    /** Rf. */
    private RandomForestClassifierTrainer rf = new RandomForestClassifierTrainer(meta)
        .withAmountOfTrees(cntOfTrees)
        .withSeed(seed)
        .withFeaturesCountSelectionStrgy(x -> 4)
        .withMaxDepth(maxDepth)
        .withMinImpurityDelta(minImpDelta)
        .withSubSampleSize(0.1);

    /** */
    @Test
    public void testNeedSplit() {
        TreeNode node = new TreeNode(1, 1);
        node.setImpurity(1000);
        assertTrue(rf.needSplit(node, Optional.of(new NodeSplit(0, 0, node.getImpurity() - minImpDelta * 1.01))));
        assertFalse(rf.needSplit(node, Optional.of(new NodeSplit(0, 0, node.getImpurity() - minImpDelta * 0.5))));
        assertFalse(rf.needSplit(node, Optional.of(new NodeSplit(0, 0, node.getImpurity()))));

        TreeNode child = node.toConditional(0, 0).get(0);
        child.setImpurity(1000);
        assertFalse(rf.needSplit(child, Optional.of(new NodeSplit(0, 0, child.getImpurity() - minImpDelta * 1.01))));
    }
}
