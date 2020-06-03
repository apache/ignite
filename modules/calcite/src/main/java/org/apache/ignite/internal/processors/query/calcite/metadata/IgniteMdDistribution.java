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

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Implementation class for {@link RelMetadataQuery#distribution(RelNode)} method call.
 */
public class IgniteMdDistribution implements MetadataHandler<BuiltInMetadata.Distribution> {
    /**
     * Metadata provider, responsible for distribution type request. It uses this implementation class under the hood.
     */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.DISTRIBUTION.method, new IgniteMdDistribution());

    /** {@inheritDoc} */
    @Override public MetadataDef<BuiltInMetadata.Distribution> getDef() {
        return BuiltInMetadata.Distribution.DEF;
    }

    /**
     * Requests actual distribution type of the given relational node.
     * @param rel Relational node.
     * @param mq Metadata query instance. Used to request appropriate metadata from node children.
     * @return Distribution type of the given relational node.
     */
    public IgniteDistribution distribution(RelNode rel, RelMetadataQuery mq) {
        return Commons.distribution(rel.getTraitSet());
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(IgniteRel rel, RelMetadataQuery mq) {
        return rel.distribution();
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(TableScan rel, RelMetadataQuery mq) {
        return rel.getTable().unwrap(IgniteTable.class).distribution();
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(Values rel, RelMetadataQuery mq) {
        return IgniteDistributions.broadcast();
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(Exchange rel, RelMetadataQuery mq) {
        return (IgniteDistribution) rel.distribution;
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(HepRelVertex rel, RelMetadataQuery mq) {
        return _distribution(rel.getCurrentRel(), mq);
    }

    /**
     * Distribution request entry point.
     * @param rel Relational node.
     * @param mq Metadata query instance.
     * @return Actual distribution of the given relational node.
     */
    public static IgniteDistribution _distribution(RelNode rel, RelMetadataQuery mq) {
        assert mq instanceof RelMetadataQueryEx;

        return (IgniteDistribution) mq.distribution(rel);
    }
}
