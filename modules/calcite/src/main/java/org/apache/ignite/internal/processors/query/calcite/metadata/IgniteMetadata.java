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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;

/**
 *
 */
public class IgniteMetadata {
    public static final RelMetadataProvider METADATA_PROVIDER =
        ChainedRelMetadataProvider.of(
            ImmutableList.of(
                IgniteMdDistribution.SOURCE,
                IgniteMdFragmentLocation.SOURCE,
                DefaultRelMetadataProvider.INSTANCE));

    public interface DistributionTraitMetadata extends Metadata {
        MetadataDef<DistributionTraitMetadata> DEF = MetadataDef.of(DistributionTraitMetadata.class,
            DistributionTraitMetadata.Handler.class, IgniteMethod.DISTRIBUTION_TRAIT.method());

        /** Determines how the rows are distributed. */
        DistributionTrait getDistributionTrait();

        /** Handler API. */
        interface Handler extends MetadataHandler<DistributionTraitMetadata> {
            DistributionTrait getDistributionTrait(RelNode r, RelMetadataQuery mq);
        }
    }

    public interface FragmentLocationMetadata extends Metadata {
        MetadataDef<FragmentLocationMetadata> DEF = MetadataDef.of(FragmentLocationMetadata.class,
            FragmentLocationMetadata.Handler.class, IgniteMethod.FRAGMENT_LOCATION.method());

        /** Determines how the rows are distributed. */
        FragmentLocation getLocation();

        /** Handler API. */
        interface Handler extends MetadataHandler<FragmentLocationMetadata> {
            FragmentLocation getLocation(RelNode r, RelMetadataQuery mq);
        }
    }
}
