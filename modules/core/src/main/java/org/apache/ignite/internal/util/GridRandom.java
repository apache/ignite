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

package org.apache.ignite.internal.util;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Random to be used from a single thread. Compatible with {@link Random} but faster.
 */
public class GridRandom extends Random {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long rnd;

    /**
     * Default constructor.
     */
    public GridRandom() {
        this(ThreadLocalRandom.current().nextLong());
    }

    /**
     * @param seed Seed.
     */
    public GridRandom(long seed) {
        setSeed(seed);
    }

    /** {@inheritDoc} */
    @Override public void setSeed(long seed) {
        rnd = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
    }

    /** {@inheritDoc} */
    @Override protected int next(int bits) {
        rnd = (rnd * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
        return (int)(rnd >>> (48 - bits));
    }
}