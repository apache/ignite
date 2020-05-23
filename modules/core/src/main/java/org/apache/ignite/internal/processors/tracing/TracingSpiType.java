/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.tracing;

/**
 * Type of the tracing spi.
 */
public enum TracingSpiType {
    /** */
    NOOP_TRACING_SPI((byte)0),

    /** */
    OPEN_CENSUS_TRACING_SPI((byte)1);

    /** Byte index of a tracing spi instance. */
    private final byte idx;

    /**
     * Constrictor
     *
     * @param idx Index.
     */
    TracingSpiType(byte idx) {
        this.idx = idx;
    }

    /**
     * @return Index.
     */
    public byte index() {
        return idx;
    }
}
