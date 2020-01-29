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
 * See {@link RelMetadataQuery}
 */
public class RelMetadataQueryEx extends RelMetadataQuery {
    /** */
    private static final RelMetadataQueryEx PROTO = new RelMetadataQueryEx();

    /** */
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

    /** */
    private IgniteMetadata.FragmentMetadata.Handler sourceDistributionHandler;

    /** */
    private IgniteMetadata.DerivedDistribution.Handler derivedDistributionsHandler;

    /**
     * Factory method.
     *
     * @return return Metadata query instance.
     */
    @SuppressWarnings("MethodOverridesStaticMethodOfSuperclass")
    public static RelMetadataQueryEx instance() {
        return new RelMetadataQueryEx(PROTO);
    }

    /**
     * Wraps an original metadata query instance and reuses its prepared handlers.
     *
     * @param mq Original metadata query instance.
     * @return Wrapped metadata query instance.
     */
    public static RelMetadataQueryEx wrap(@NotNull RelMetadataQuery mq) {
        if (mq.getClass() == RelMetadataQueryEx.class)
            return (RelMetadataQueryEx) mq;

        return new RelMetadataQueryEx(mq);
    }

    /**
     * @param parent Parent metadata query instance.
     */
    private RelMetadataQueryEx(@NotNull RelMetadataQueryEx parent) {
        super(PROVIDER, parent);

        sourceDistributionHandler = parent.sourceDistributionHandler;
        derivedDistributionsHandler = parent.derivedDistributionsHandler;
    }

    /**
     * @param parent Parent metadata query instance.
     */
    private RelMetadataQueryEx(@NotNull RelMetadataQuery parent) {
        super(PROVIDER, parent);

        sourceDistributionHandler = PROTO.sourceDistributionHandler;
        derivedDistributionsHandler = PROTO.derivedDistributionsHandler;
    }

    /**
     * Constructs query prototype.
     */
    private RelMetadataQueryEx() {
        super(JaninoRelMetadataProvider.DEFAULT, RelMetadataQuery.EMPTY);

        sourceDistributionHandler = initialHandler(IgniteMetadata.FragmentMetadata.Handler.class);
        derivedDistributionsHandler = initialHandler(IgniteMetadata.DerivedDistribution.Handler.class);
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
                return sourceDistributionHandler.getFragmentInfo(rel, this);
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
}
