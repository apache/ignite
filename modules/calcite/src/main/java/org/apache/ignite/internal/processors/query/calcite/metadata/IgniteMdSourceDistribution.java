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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.query.calcite.exchange.Receiver;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata.TaskDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.splitter.SourceDistribution;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;
import org.apache.ignite.internal.util.GridIntList;

/**
 *
 */
public class IgniteMdSourceDistribution implements MetadataHandler<TaskDistribution> {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(IgniteMethod.TASK_DISTRIBUTION.method(), new IgniteMdSourceDistribution());

    @Override public MetadataDef<TaskDistribution> getDef() {
        return TaskDistribution.DEF;
    }

    public SourceDistribution distribution(RelNode rel, RelMetadataQuery mq) {
        throw new AssertionError();
    }

    public SourceDistribution distribution(RelSubset rel, RelMetadataQuery mq) {
        throw new AssertionError();
    }

    public SourceDistribution distribution(SingleRel rel, RelMetadataQuery mq) {
        return distribution_(rel.getInput(), mq);
    }

    public SourceDistribution distribution(BiRel rel, RelMetadataQuery mq) {
        return merge(distribution_(rel.getLeft(), mq), distribution_(rel.getRight(), mq));
    }

    public SourceDistribution distribution(Receiver rel, RelMetadataQuery mq) {
        SourceDistribution res = new SourceDistribution();

        res.remoteInputs.add(rel);

        return res;
    }

    public SourceDistribution distribution(IgniteLogicalTableScan rel, RelMetadataQuery mq) {
        return rel.tableDistribution();
    }

    public static SourceDistribution distribution_(RelNode rel, RelMetadataQuery mq) {
        return rel.metadata(TaskDistribution.class, mq).distribution();
    }

    private static SourceDistribution merge(SourceDistribution left, SourceDistribution right) {
        SourceDistribution res = new SourceDistribution();

        res.remoteInputs = merge(left.remoteInputs, right.remoteInputs);
        res.partitionMapping = merge(left.partitionMapping, right.partitionMapping);
        res.localInputs = merge(left.localInputs, right.localInputs);

        return res;
    }

    private static <T> List<T> merge(List<T> left, List<T> right) {
        if (left == null)
            return right;

        if (right != null)
            left.addAll(right);

        return left;
    }

    private static GridIntList merge(GridIntList left, GridIntList right) {
        if (left == null)
            return right;

        if (right != null)
            left.addAll(right);

        return left;
    }

    private static Map<ClusterNode, int[]> merge(Map<ClusterNode, int[]> left, Map<ClusterNode, int[]> right) {
        if (left == null)
            return right;

        if (right == null)
            return left;

        Map<ClusterNode, int[]> res = new HashMap<>(Math.min(left.size(), right.size()));

        Set<ClusterNode> keys = new HashSet<>(left.keySet());

        keys.retainAll(right.keySet());

        for (ClusterNode node : keys) {
            int[] leftParts  = left.get(node);
            int[] rightParts = right.get(node);

            int[] nodeParts = new int[Math.min(leftParts.length, rightParts.length)];

            int i = 0, j = 0, k = 0;

            while (i < leftParts.length && j < rightParts.length) {
                if (leftParts[i] < rightParts[j])
                    i++;
                else if (rightParts[j] < leftParts[i])
                    j++;
                else {
                    nodeParts[k++] = leftParts[i];

                    i++;
                    j++;
                }
            }

            if (k > 0)
                res.put(node, k < nodeParts.length ? Arrays.copyOf(nodeParts, k) : nodeParts);
        }

        return res;
    }
}
