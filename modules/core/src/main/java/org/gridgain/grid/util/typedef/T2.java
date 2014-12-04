/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.typedef;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.lang.*;

import java.io.*;

/**
 * Defines {@code alias} for {@link org.apache.ignite.lang.IgniteBiTuple} by extending it. Since Java doesn't provide type aliases
 * (like Scala, for example) we resort to these types of measures. This is intended to provide for more
 * concise code in cases when readability won't be sacrificed. For more information see {@link org.apache.ignite.lang.IgniteBiTuple}.
 * @see GridFunc
 * @see GridTuple
 */
public class T2<V1, V2> extends IgniteBiTuple<V1, V2> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public T2() {
        // No-op.
    }

    /**
     * Fully initializes this tuple.
     *
     * @param val1 First value.
     * @param val2 Second value.
     */
    public T2(V1 val1, V2 val2) {
        super(val1, val2);
    }
}
