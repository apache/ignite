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

import java.util.List;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.exchange.Receiver;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata.SourceDistributionMetadata;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.splitter.PartitionsDistribution;
import org.apache.ignite.internal.processors.query.calcite.splitter.SourceDistribution;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class IgniteMdSourceDistribution implements MetadataHandler<SourceDistributionMetadata> {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(IgniteMethod.SOURCE_DISTRIBUTION.method(), new IgniteMdSourceDistribution());

    @Override public MetadataDef<SourceDistributionMetadata> getDef() {
        return SourceDistributionMetadata.DEF;
    }

    public SourceDistribution getSourceDistribution(RelNode rel, RelMetadataQuery mq) {
        throw new AssertionError();
    }

    public SourceDistribution getSourceDistribution(RelSubset rel, RelMetadataQuery mq) {
        throw new AssertionError();
    }

    public SourceDistribution getSourceDistribution(SingleRel rel, RelMetadataQuery mq) {
        return distribution(rel.getInput(), mq);
    }

    public SourceDistribution getSourceDistribution(BiRel rel, RelMetadataQuery mq) {
        mq = RelMetadataQueryEx.wrap(mq);

        return merge(distribution(rel.getLeft(), mq), distribution(rel.getRight(), mq));
    }

    public SourceDistribution getSourceDistribution(Receiver rel, RelMetadataQuery mq) {
        SourceDistribution res = new SourceDistribution();

        res.remoteInputs = F.asList(rel);

        return res;
    }

    public SourceDistribution getSourceDistribution(IgniteLogicalTableScan rel, RelMetadataQuery mq) {
        return rel.tableDistribution();
    }

    public static SourceDistribution distribution(RelNode rel, RelMetadataQuery mq) {
        return RelMetadataQueryEx.wrap(mq).getSourceDistribution(rel);
    }

    private static SourceDistribution merge(SourceDistribution left, SourceDistribution right) {
        SourceDistribution res = new SourceDistribution();

        res.partitionMapping = merge(left.partitionMapping, right.partitionMapping);
        res.remoteInputs = merge(left.remoteInputs, right.remoteInputs);
        res.localInputs = merge(left.localInputs, right.localInputs);

        return res;
    }

    private static PartitionsDistribution merge(PartitionsDistribution left, PartitionsDistribution right) {
        if (left == null)
            return right;
        if (right == null)
            return left;

        return left.mergeWith(right);
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
}
