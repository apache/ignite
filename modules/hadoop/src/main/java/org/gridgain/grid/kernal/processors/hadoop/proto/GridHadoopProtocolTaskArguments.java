/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.proto;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Task arguments.
 */
public class GridHadoopProtocolTaskArguments implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Arguments. */
    private Object[] args;

    /**
     * {@link Externalizable} support.
     */
    public GridHadoopProtocolTaskArguments() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param args Arguments.
     */
    public GridHadoopProtocolTaskArguments(Object... args) {
        this.args = args;
    }

    /**
     * @param idx Argument index.
     * @return Argument.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <T> T get(int idx) {
        return (args != null && args.length > idx) ? (T)args[idx] : null;
    }

    /**
     * @return Size.
     */
    public int size() {
        return args != null ? args.length : 0;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeArray(out, args);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        args = U.readArray(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopProtocolTaskArguments.class, this);
    }
}
