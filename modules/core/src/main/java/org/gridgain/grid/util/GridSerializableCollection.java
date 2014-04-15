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
 * Makes {@link AbstractCollection} as {@link Serializable} and is
 * useful for making anonymous serializable collections. It has no
 * extra logic or state in addition to {@link AbstractCollection}.
 */
public abstract class GridSerializableCollection<E> extends AbstractCollection<E> implements Serializable {
    private static final long serialVersionUID = 0L;

    // No-op.
}
