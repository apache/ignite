/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import java.util.*;
import java.util.concurrent.*;

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
