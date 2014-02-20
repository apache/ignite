// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import java.io.*;
import java.util.*;

/**
 * Makes {@link AbstractMap} as {@link Serializable} and is
 * useful for making anonymous serializable maps. It has no extra logic or state in addition
 * to {@link AbstractMap}.
 * <b>NOTE:</b> methods {@link #get(Object)}, {@link #remove(Object)} and
 * {@link #containsKey(Object)} implemented in {@link AbstractMap} <b>fully iterate through
 * collection</b> so you need to make sure to override these methods if it's possible to create
 * efficient implementations.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridSerializableMap<K, V> extends AbstractMap<K, V> implements Serializable {
    // No-op.
}
