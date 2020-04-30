/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;

/**
 * Utility class, holding metadata related interfaces and metadata providers.
 */
public class IgniteMetadata {
    public static final RelMetadataProvider METADATA_PROVIDER =
        ChainedRelMetadataProvider.of(
            ImmutableList.of(
                IgniteMdDerivedDistribution.SOURCE,
                IgniteMdDistribution.SOURCE,
                IgniteMdNodesMapping.SOURCE,
                DefaultRelMetadataProvider.INSTANCE));

    public interface NodesMappingMetadata extends Metadata {
        MetadataDef<NodesMappingMetadata> DEF = MetadataDef.of(NodesMappingMetadata.class,
            NodesMappingMetadata.Handler.class, IgniteMethod.NODES_MAPPING.method());

        /** Determines how the rows are distributed. */
        NodesMapping nodesMapping();

        /** Handler API. */
        interface Handler extends MetadataHandler<NodesMappingMetadata> {
            NodesMapping nodesMapping(RelNode r, RelMetadataQuery mq);
        }
    }

    public interface DerivedDistribution extends Metadata {
        MetadataDef<DerivedDistribution> DEF = MetadataDef.of(DerivedDistribution.class,
            DerivedDistribution.Handler.class, IgniteMethod.DERIVED_DISTRIBUTIONS.method());

        List<IgniteDistribution> deriveDistributions();

        /** Handler API. */
        interface Handler extends MetadataHandler<DerivedDistribution> {
            List<IgniteDistribution> deriveDistributions(RelNode r, RelMetadataQuery mq);
        }
    }
}
