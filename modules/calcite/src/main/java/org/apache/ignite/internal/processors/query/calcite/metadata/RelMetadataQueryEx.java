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
import java.util.Set;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;

/**
 * See {@link RelMetadataQuery}
 */
public class RelMetadataQueryEx extends RelMetadataQuery {
    static {
        JaninoRelMetadataProvider.DEFAULT.register(
            ImmutableList.of(
                IgniteExchange.class,
                IgniteReceiver.class,
                IgniteSender.class,
                IgniteFilter.class,
                IgniteProject.class,
                IgniteJoin.class,
                IgniteTableScan.class,
                IgniteValues.class,
                IgniteTableModify.class)); // TODO add sort
    }

    /** */
    private static final IgniteMetadata.FragmentMetadata.Handler SOURCE_DISTRIBUTION_INITIAL_HANDLER =
        initialHandler(IgniteMetadata.FragmentMetadata.Handler.class);

    /** */
    private static final IgniteMetadata.DerivedDistribution.Handler DERIVED_DISTRIBUTIONS_INITIAL_HANDLER =
        initialHandler(IgniteMetadata.DerivedDistribution.Handler.class);

    /** */
    private static final IgniteMetadata.DerivedTraitSet.Handler DERIVED_TRAIT_SETS_INITIAL_HANDLER =
        initialHandler(IgniteMetadata.DerivedTraitSet.Handler.class);

    /** */
    private IgniteMetadata.FragmentMetadata.Handler sourceDistributionHandler;

    /** */
    private IgniteMetadata.DerivedDistribution.Handler derivedDistributionsHandler;

    /** */
    private IgniteMetadata.DerivedTraitSet.Handler derivedTraitSetsHandler;

    /**
     * Factory method.
     *
     * @return return Metadata query instance.
     */
    public static RelMetadataQueryEx create() {
        return create(IgniteMetadata.METADATA_PROVIDER);
    }

    /**
     * Factory method.
     *
     * @return return Metadata query instance.
     */
    public static RelMetadataQueryEx create(RelMetadataProvider metadataProvider) {
        THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(metadataProvider));
        try {
            return new RelMetadataQueryEx();
        }
        finally {
            THREAD_PROVIDERS.remove();
        }
    }

    /** */
    private RelMetadataQueryEx() {
        sourceDistributionHandler = SOURCE_DISTRIBUTION_INITIAL_HANDLER;
        derivedDistributionsHandler = DERIVED_DISTRIBUTIONS_INITIAL_HANDLER;
        derivedTraitSetsHandler = DERIVED_TRAIT_SETS_INITIAL_HANDLER;
    }

    /**
     * Calculates fragment meta information, the given relation node is a root of.
     *
     * @param rel Relational node.
     * @return Fragment meta information.
     */
    public FragmentInfo getFragmentInfo(RelNode rel) {
        for (;;) {
            try {
                return sourceDistributionHandler.fragmentInfo(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                sourceDistributionHandler = revise(e.relClass, IgniteMetadata.FragmentMetadata.DEF);
            }
        }
    }

    /**
     * Requests possible distribution types of given relational node. In case the node is logical and
     * @param rel Relational node.
     * @return List of distribution types the given relational node may have.
     */
    public List<IgniteDistribution> derivedDistributions(RelNode rel) {
        for (;;) {
            try {
                return derivedDistributionsHandler.deriveDistributions(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                derivedDistributionsHandler = revise(e.relClass, IgniteMetadata.DerivedDistribution.DEF);
            }
        }
    }

    /**
     * Requests all possible trait sets fot the given relational node.
     * @param rel Relational node.
     * @return List of possible trait sets the given relational node may have.
     */
    public Set<RelTraitSet> deriveTraitSets(RelNode rel) {
        for (;;) {
            try {
                return derivedTraitSetsHandler.deriveTraitSets(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                derivedTraitSetsHandler = revise(e.relClass, IgniteMetadata.DerivedTraitSet.DEF);
            }
        }
    }
}
