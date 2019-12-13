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

package org.apache.ignite.internal.processors.query.calcite.splitter;

import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;

/**
 *
 */
public interface RelSource {
    /**
     * @return Exchange id, has to be unique in scope of query.
     */
    long exchangeId();

    /**
     * @return Source mapping.
     */
    NodesMapping mapping();

    /**
     * @param mapping Target mapping.
     * @param distribution Target distribution.
     * @param ctx Context.
     * @param mq Metadata query instance.
     */
    default void init(NodesMapping mapping, IgniteDistribution distribution, PlannerContext ctx, RelMetadataQuery mq) {
        // No-op
    }
}
