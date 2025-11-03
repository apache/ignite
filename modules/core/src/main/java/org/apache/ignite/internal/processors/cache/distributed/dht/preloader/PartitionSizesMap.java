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

import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** Partition sizes map. */
public class PartitionSizesMap implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 514;

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
    public PartitionSizesMap(@Nullable Map<Integer, Long> partsSizes) {
        this.partsSizes = partsSizes;
    }

    /**
     * @return Partition sizes map.
     */
    public Map<Integer, Long> partitionSizes() {
        return F.emptyIfNull(partsSizes);
    }

    /**
     * Used only for serialization.
     * Since the code generation framework currently does not support null values for primitive wrappers,
     * we use {@link Long#MIN_VALUE} instead of null.
     *
     * @return Partition sizes map.
     */
    public @Nullable Map<Integer, Long> partitionSizesMap() {
        return partsSizes == null ? null : F.viewReadOnly(partsSizes, v -> v != null ? v : Long.MIN_VALUE);
    }

    /**
     * Used only for deserialization.
     * Since the code generation framework currently does not support null values for primitive wrappers,
     * we use {@link Long#MIN_VALUE} instead of null.
     *
     * @param partsSizes Partition sizes map.
     */
    public void partitionSizesMap(@Nullable Map<Integer, Long> partsSizes) {
        this.partsSizes = partsSizes == null ? null : F.viewReadOnly(partsSizes, v -> v != Long.MIN_VALUE ? v : null);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
