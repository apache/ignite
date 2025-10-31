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

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** Partition sizes map. */
public class PartitionSizesMap implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 509;

    /** Partition sizes map. */
    @Order(value = 0, method = "partitionSizesMap")
    private @Nullable Map<Integer, Long> partsSizes;

    /** Default constructor. */
    public PartitionSizesMap() {
        // No-op.
    }

    /**
     * @param partsSizes Partition sizes map.
     */
    public PartitionSizesMap(Map<Integer, Long> partsSizes) {
        this.partsSizes = partsSizes;
    }

    /**
     * @return Partition sizes map.
     */
    public Map<Integer, Long> partitionSizes() {
        return partsSizes != null ? partsSizes : Collections.emptyMap();
    }

    /**
     * Used only for serialization.
     * Since the code generation framework currently does not support null values for primitive wrappers,
     * we use {@link Long#MIN_VALUE} instead of null.
     *
     * @return Partition sizes map.
     */
    public @Nullable Map<Integer, Long> partitionSizesMap() {
        if (partsSizes == null)
            return null;

        return partsSizes.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() != null ? e.getValue() : Long.MIN_VALUE));
    }

    /**
     * Used only for deserialization.
     *
     * @param partsSizes Partition sizes map.
     */
    public void partitionSizesMap(@Nullable Map<Integer, Long> partsSizes) {
        if (partsSizes == null)
            this.partsSizes = partsSizes;
        else
            this.partsSizes = partsSizes.entrySet().stream()
                .collect(Collectors.groupingBy(
                    Map.Entry::getKey,
                    Collectors.mapping(
                        e -> e.getValue() != Long.MIN_VALUE ? e.getValue() : null,
                        Collectors.reducing(null, (v1, v2) -> v1)
                    )
                ));
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
