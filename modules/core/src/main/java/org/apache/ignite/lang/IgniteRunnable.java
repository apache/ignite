/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.lang;

import org.gridgain.grid.compute.*;

import java.io.*;

/**
 * Grid-aware adapter for {@link Runnable} implementations. It adds {@link Serializable} interface
 * to {@link Runnable} object. Use this class for executing distributed computations on the grid,
 * like in {@link GridCompute#run(Runnable)} method.
 */
public interface IgniteRunnable extends Runnable, Serializable {
    // No-op.
}
