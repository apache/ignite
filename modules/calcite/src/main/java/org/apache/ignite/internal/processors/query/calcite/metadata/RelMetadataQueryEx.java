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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class RelMetadataQueryEx extends RelMetadataQuery {
    private static final RelMetadataQueryEx PROTO = new RelMetadataQueryEx();
    public static final JaninoRelMetadataProvider PROVIDER = JaninoRelMetadataProvider.of(IgniteMetadata.METADATA_PROVIDER);

    static {
        PROVIDER.register(ImmutableList.of(
                IgniteExchange.class,
                IgniteReceiver.class,
                IgniteSender.class,
                IgniteFilter.class,
                IgniteProject.class,
                IgniteJoin.class,
                IgniteTableScan.class));
    }

    private IgniteMetadata.FragmentMetadata.Handler sourceDistributionHandler;
    private IgniteMetadata.DerivedDistribution.Handler derivedDistributionsHandler;

    @SuppressWarnings("MethodOverridesStaticMethodOfSuperclass")
    public static RelMetadataQueryEx instance() {
        return new RelMetadataQueryEx(PROTO);
    }

    public static RelMetadataQueryEx wrap(@NotNull RelMetadataQuery mq) {
        if (mq.getClass() == RelMetadataQueryEx.class)
            return (RelMetadataQueryEx) mq;

        return new RelMetadataQueryEx(mq);
    }

    private RelMetadataQueryEx(@NotNull RelMetadataQueryEx parent) {
        super(PROVIDER, parent);

        sourceDistributionHandler = parent.sourceDistributionHandler;
        derivedDistributionsHandler = parent.derivedDistributionsHandler;
    }

    private RelMetadataQueryEx(@NotNull RelMetadataQuery parent) {
        super(PROVIDER, parent);

        sourceDistributionHandler = PROTO.sourceDistributionHandler;
        derivedDistributionsHandler = PROTO.derivedDistributionsHandler;
    }

    private RelMetadataQueryEx() {
        super(JaninoRelMetadataProvider.DEFAULT, RelMetadataQuery.EMPTY);

        sourceDistributionHandler = initialHandler(IgniteMetadata.FragmentMetadata.Handler.class);
        derivedDistributionsHandler = initialHandler(IgniteMetadata.DerivedDistribution.Handler.class);
    }

    public FragmentInfo getFragmentLocation(RelNode rel) {
        for (;;) {
            try {
                return sourceDistributionHandler.getFragmentInfo(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                sourceDistributionHandler = revise(e.relClass, IgniteMetadata.FragmentMetadata.DEF);
            }
        }
    }

    public List<IgniteDistribution> derivedDistributions(RelNode rel) {
        for (;;) {
            try {
                return derivedDistributionsHandler.deriveDistributions(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                derivedDistributionsHandler = revise(e.relClass, IgniteMetadata.DerivedDistribution.DEF);
            }
        }
    }
}
