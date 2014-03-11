/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Simple extension over {@link GridTuple3} for three objects of the same type.
 */
public class GridTriple<T> extends GridTuple3<T, T, T> {
    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridTriple() {
        // No-op.
    }

    /**
     * Creates triple with given objects.
     *
     * @param t1 First object in triple.
     * @param t2 Second object in triple.
     * @param t3 Third object in triple.
     */
    public GridTriple(@Nullable T t1, @Nullable T t2, @Nullable T t3) {
        super(t1, t2, t3);
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"CloneDoesntDeclareCloneNotSupportedException"})
    @Override public Object clone() {
        return super.clone();
    }
}
