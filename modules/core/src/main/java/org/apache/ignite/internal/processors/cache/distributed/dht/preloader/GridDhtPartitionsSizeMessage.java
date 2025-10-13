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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** Message for partitions sizes map. */
public class GridDhtPartitionsSizeMessage implements Message, Serializable {
    /** Type code. */
    public static final short TYPE_CODE = 517;

    /** */
    private static final long serialVersionUID = 0L;

    /** Partitions sizes map. */
    @Order(value = 0, method = "partitionSizesMap")
    private @Nullable Map<Integer, Long> partsSizes;

    /** Constructor. */
    public GridDhtPartitionsSizeMessage() {
        // No-op.
    }

    /**
     * @param partsSizes Partitions sizes map.
     */
    public GridDhtPartitionsSizeMessage(@Nullable Map<Integer, Long> partsSizes) {
        this.partsSizes = partsSizes;
    }

    /**
     * @return Partitions sizes map.
     */
    public @Nullable Map<Integer, Long> partitionSizes() {
        return partsSizes;
    }

    /**
     * @return TODO
     */
    public @Nullable Map<Integer, Long> partitionSizesMap() {
        if (partsSizes == null)
            return null;

        return partsSizes.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() != null ? e.getValue() : Long.MIN_VALUE));
    }

    /**
     * @param partsSizes Partitions sizes map.
     */
    public void partitionSizesMap(@Nullable Map<Integer, Long> partsSizes) {
        if (partsSizes == null) {
            this.partsSizes = partsSizes;

            return;
        }

        this.partsSizes = partsSizes.entrySet().stream()
            .collect(Collectors.groupingBy(
                Map.Entry::getKey,
                Collectors.mapping(e -> e.getValue() != Long.MIN_VALUE ? e.getValue() : null, Collectors.reducing(null, (v1, v2) -> v1))
            ));
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
