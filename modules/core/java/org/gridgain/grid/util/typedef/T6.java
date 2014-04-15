/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.typedef;

import org.gridgain.grid.util.lang.*;

import java.io.*;

/**
 * Defines {@code alias} for {@link GridTuple6} by extending it. Since Java doesn't provide type aliases
 * (like Scala, for example) we resort to these types of measures. This is intended to provide for more
 * concise code in cases when readability won't be sacrificed. For more information see {@link GridTuple6}.
 * @see GridFunc
 * @see GridTuple
 */
public class T6<V1, V2, V3, V4, V5, V6> extends GridTuple6<V1, V2, V3, V4, V5, V6> {
    private static final long serialVersionUID = -2561832852576348876L;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public T6() {
        // No-op.
    }

    /**
     * Fully initializes this tuple.
     *
     * @param v1 First value.
     * @param v2 Second value.
     * @param v3 Third value.
     * @param v4 Forth value.
     * @param v5 Fifth value.
     * @param v6 Sixth value.
     */
    public T6(V1 v1, V2 v2, V3 v3, V4 v4, V5 v5, V6 v6) {
        super(v1, v2, v3, v4, v5, v6);
    }
}
