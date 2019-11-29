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

import java.io.Serializable;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;

/**
 *
 */
public class TargetImpl implements Target, Serializable {
    private final long exchangeId;
    private final NodesMapping mapping;
    private final DistributionTrait distribution;

    public TargetImpl(long exchangeId, NodesMapping mapping, DistributionTrait distribution) {
        this.exchangeId = exchangeId;
        this.mapping = mapping;
        this.distribution = distribution;
    }

    @Override public long exchangeId() {
        return exchangeId;
    }

    @Override public NodesMapping mapping() {
        return mapping;
    }

    @Override public DistributionTrait distribution() {
        return distribution;
    }
}
