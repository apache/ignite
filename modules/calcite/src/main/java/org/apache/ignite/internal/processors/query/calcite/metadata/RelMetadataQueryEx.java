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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.splitter.SourceDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class RelMetadataQueryEx extends RelMetadataQuery {
    private static final RelMetadataQueryEx PROTO = new RelMetadataQueryEx();
    private static final JaninoRelMetadataProvider PROVIDER = JaninoRelMetadataProvider.of(IgniteMetadata.METADATA_PROVIDER);

    private IgniteMetadata.DistributionTraitMetadata.Handler distributionTraitHandler;
    private IgniteMetadata.SourceDistributionMetadata.Handler sourceDistributionHandler;

    private RelMetadataQueryEx() {
        super(JaninoRelMetadataProvider.DEFAULT, RelMetadataQuery.EMPTY);

        distributionTraitHandler = initialHandler(IgniteMetadata.DistributionTraitMetadata.Handler.class);
        sourceDistributionHandler = initialHandler(IgniteMetadata.SourceDistributionMetadata.Handler.class);
    }

    protected RelMetadataQueryEx(JaninoRelMetadataProvider metadataProvider, RelMetadataQueryEx prototype) {
        super(metadataProvider, prototype);

        distributionTraitHandler = prototype.distributionTraitHandler;
        sourceDistributionHandler = prototype.sourceDistributionHandler;
    }

    protected RelMetadataQueryEx(JaninoRelMetadataProvider metadataProvider, RelMetadataQuery parent) {
        super(metadataProvider, parent);

        distributionTraitHandler = PROTO.distributionTraitHandler;
        sourceDistributionHandler = PROTO.sourceDistributionHandler;
    }

    @SuppressWarnings("MethodOverridesStaticMethodOfSuperclass")
    public static RelMetadataQueryEx instance() {
        return new RelMetadataQueryEx(PROVIDER, PROTO);
    }

    public static RelMetadataQueryEx wrap(@NotNull RelMetadataQuery mq) {
        if (mq.getClass() == RelMetadataQueryEx.class)
            return (RelMetadataQueryEx) mq;

        return new RelMetadataQueryEx(PROVIDER, mq);
    }

    public SourceDistribution getSourceDistribution(RelNode rel) {
        for (;;) {
            try {
                return sourceDistributionHandler.getSourceDistribution(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                sourceDistributionHandler = revise(e.relClass, IgniteMetadata.SourceDistributionMetadata.DEF);
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
