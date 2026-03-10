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
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Map for storing integer to long value mapping (e.g. partition size or partition history counter for
 * a partition of a given id).
 */
public class IntLongMap implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 514;

    /** Map. */
    @Order(0)
    @Nullable Map<Integer, Long> map;

    /** Default constructor. */
    public IntLongMap() {
        // No-op.
    }

    /**
     * @param map Map.
     */
    public IntLongMap(@Nullable Map<Integer, Long> map) {
        this.map = map;
    }

    /**
     * @return Map.
     */
    public @Nullable Map<Integer, Long> map() {
        return map;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
