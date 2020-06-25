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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMdAllPredicates;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdColumnOrigins;
import org.apache.calcite.rel.metadata.RelMdColumnUniqueness;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMdExplainVisibility;
import org.apache.calcite.rel.metadata.RelMdExpressionLineage;
import org.apache.calcite.rel.metadata.RelMdMaxRowCount;
import org.apache.calcite.rel.metadata.RelMdMemory;
import org.apache.calcite.rel.metadata.RelMdMinRowCount;
import org.apache.calcite.rel.metadata.RelMdNodeTypes;
import org.apache.calcite.rel.metadata.RelMdParallelism;
import org.apache.calcite.rel.metadata.RelMdPopulationSize;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdSize;
import org.apache.calcite.rel.metadata.RelMdTableReferences;
import org.apache.calcite.rel.metadata.RelMdUniqueKeys;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;

/**
 * Utility class, holding metadata related interfaces and metadata providers.
 */
public class IgniteMetadata {
    public static final RelMetadataProvider METADATA_PROVIDER =
        ChainedRelMetadataProvider.of(
            ImmutableList.of(
                // Ignite specific providers
                IgniteMdNodesMapping.SOURCE,

                // Ignite overriden providers
                IgniteMdDistribution.SOURCE,
                IgniteMdPercentageOriginalRows.SOURCE,
                IgniteMdCumulativeCost.SOURCE,
                IgniteMdNonCumulativeCost.SOURCE,
                IgniteMdRowCount.SOURCE,

                // Basic providers
                RelMdColumnOrigins.SOURCE,
                RelMdExpressionLineage.SOURCE,
                RelMdTableReferences.SOURCE,
                RelMdNodeTypes.SOURCE,
                RelMdMaxRowCount.SOURCE,
                RelMdMinRowCount.SOURCE,
                RelMdUniqueKeys.SOURCE,
                RelMdColumnUniqueness.SOURCE,
                RelMdPopulationSize.SOURCE,
                RelMdSize.SOURCE,
                RelMdParallelism.SOURCE,
                RelMdMemory.SOURCE,
                RelMdDistinctRowCount.SOURCE,
                RelMdSelectivity.SOURCE,
                RelMdExplainVisibility.SOURCE,
                RelMdPredicates.SOURCE,
                RelMdAllPredicates.SOURCE,
                RelMdCollation.SOURCE));

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
}
