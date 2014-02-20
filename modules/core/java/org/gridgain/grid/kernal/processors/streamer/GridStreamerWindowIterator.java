// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Helper iterator extension which prevents regular element remove and adds removex() method tracking which element
 * was actually removed.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridStreamerWindowIterator<T> implements Iterator<T> {
    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * Remove element from the underlying collection and return removed element.
     *
     * @return Removed element or {@code null} in case no deletion occurred.
     */
    @Nullable public abstract T removex();
}
