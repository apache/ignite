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

/**
 *
 */
public class RelSourceImpl implements RelSource, Serializable {
    private final long exchangeId;
    private final NodesMapping mapping;

    public RelSourceImpl(long exchangeId, NodesMapping mapping) {
        this.exchangeId = exchangeId;
        this.mapping = mapping;
    }

    @Override public long exchangeId() {
        return exchangeId;
    }

    @Override public NodesMapping mapping() {
        return mapping;
    }
}
