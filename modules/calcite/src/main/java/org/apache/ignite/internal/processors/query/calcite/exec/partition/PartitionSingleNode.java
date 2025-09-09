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

package org.apache.ignite.internal.processors.query.calcite.exec.partition;

import java.util.Collection;
import com.google.common.collect.ImmutableList;

/** */
abstract class PartitionSingleNode implements PartitionNode {
    /** */
    private final int cacheId;

    /** */
    protected PartitionSingleNode(int cacheId) {
        this.cacheId = cacheId;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> apply(PartitionPruningContext ctx) {
        Integer part = applySingle(ctx);

        return part == null ? ImmutableList.of() : ImmutableList.of(part);
    }

    /** */
    abstract Integer applySingle(PartitionPruningContext ctx);

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return cacheId;
    }
}
