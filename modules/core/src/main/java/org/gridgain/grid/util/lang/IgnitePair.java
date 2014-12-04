/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;
import java.io.*;

/**
 * Simple extension over {@link org.gridgain.grid.lang.IgniteBiTuple} for pair of objects of the same type.
 */
public class IgnitePair<T> extends IgniteBiTuple<T, T> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgnitePair() {
        // No-op.
    }

    /**
     * Creates pair with given objects.
     *
     * @param t1 First object in pair.
     * @param t2 Second object in pair.
     */
    public IgnitePair(@Nullable T t1, @Nullable T t2) {
        super(t1, t2);
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"CloneDoesntDeclareCloneNotSupportedException"})
    @Override public Object clone() {
        return super.clone();
    }
}
