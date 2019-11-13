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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.Receiver;
import org.apache.ignite.internal.processors.query.calcite.rel.Sender;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class RelMetadataQueryEx extends RelMetadataQuery {
    private static final RelMetadataQueryEx PROTO = new RelMetadataQueryEx();
    private static final JaninoRelMetadataProvider PROVIDER = JaninoRelMetadataProvider.of(IgniteMetadata.METADATA_PROVIDER);

    static {
        PROVIDER.register(Arrays.asList(
            IgniteExchange.class,
            IgniteFilter.class,
            IgniteHashJoin.class,
            IgniteProject.class,
            IgniteTableScan.class,
            Receiver.class,
            Sender.class
        ));
    }

    private IgniteMetadata.DistributionTraitMetadata.Handler distributionTraitHandler;
    private IgniteMetadata.FragmentLocationMetadata.Handler sourceDistributionHandler;

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

        distributionTraitHandler = parent.distributionTraitHandler;
        sourceDistributionHandler = parent.sourceDistributionHandler;
    }

    private RelMetadataQueryEx(@NotNull RelMetadataQuery parent) {
        super(PROVIDER, parent);

        distributionTraitHandler = PROTO.distributionTraitHandler;
        sourceDistributionHandler = PROTO.sourceDistributionHandler;
    }

    private RelMetadataQueryEx() {
        super(JaninoRelMetadataProvider.DEFAULT, RelMetadataQuery.EMPTY);

        distributionTraitHandler = initialHandler(IgniteMetadata.DistributionTraitMetadata.Handler.class);
        sourceDistributionHandler = initialHandler(IgniteMetadata.FragmentLocationMetadata.Handler.class);
    }

    public FragmentLocation getFragmentLocation(RelNode rel) {
        for (;;) {
            try {
                return sourceDistributionHandler.getLocation(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                sourceDistributionHandler = revise(e.relClass, IgniteMetadata.FragmentLocationMetadata.DEF);
            }
        }
    }

    public DistributionTrait getDistributionTrait(RelNode rel) {
        for (;;) {
            try {
                return distributionTraitHandler.getDistributionTrait(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                distributionTraitHandler = revise(e.relClass, IgniteMetadata.DistributionTraitMetadata.DEF);
            }
        }
    }
}
