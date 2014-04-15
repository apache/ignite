/* @java.file.header */

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
 * Makes {@link AbstractSet} as {@link Serializable} and is
 * useful for making anonymous serializable sets. It has no extra logic or state in
 * addition to {@link AbstractSet}.
 * <p>
 * Note that methods {@link #contains(Object)} and {@link #remove(Object)} implemented
 * in {@link AbstractCollection} fully iterate through collection so you need to make
 * sure to override these methods if it's possible to create efficient implementations.
 */
public abstract class GridSerializableSet<E> extends AbstractSet<E> implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    // No-op.
}
