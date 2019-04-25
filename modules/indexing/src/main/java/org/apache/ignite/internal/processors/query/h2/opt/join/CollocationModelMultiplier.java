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

package org.apache.ignite.internal.processors.query.h2.opt.join;

/**
 * Multiplier for different collocation types.
 */
public enum CollocationModelMultiplier {
    /** Tables are collocated, cheap. */
    COLLOCATED(1),

    /** */
    UNICAST(50),

    /** */
    BROADCAST(200),

    /** Force REPLICATED tables to be at the end of join sequence. */
    REPLICATED_NOT_LAST(10_000);

    /** Multiplier value. */
    private final int multiplier;

    /**
     * Constructor.
     *
     * @param multiplier Multiplier value.
     */
    CollocationModelMultiplier(int multiplier) {
        this.multiplier = multiplier;
    }

    /**
     * @return Multiplier value.
     */
    public int multiplier() {
        return multiplier;
    }
}
