/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.lang;

import java.io.*;
import java.util.concurrent.*;

/**
 * Grid-aware adapter for {@link Callable} implementations. It adds {@link Serializable} interface
 * to {@link Callable} object. Use this class for executing distributed computations on the grid,
 * like in {@link org.apache.ignite.IgniteCompute#call(Callable)} method.
 */
public interface IgniteCallable<V> extends Callable<V>, Serializable {
}
