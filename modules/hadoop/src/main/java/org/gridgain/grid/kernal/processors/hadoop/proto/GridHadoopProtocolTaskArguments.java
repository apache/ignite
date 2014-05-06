/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.proto;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Task arguments.
 */
public class GridHadoopProtocolTaskArguments implements Externalizable {
    /** Arguments. */
    private List<Object> args;

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
        if (args == null) {
            this.args = Collections.emptyList();
        }
        else {
            this.args = new ArrayList<>(args.length);

            Collections.addAll(this.args, args);
        }
    }

    /**
     * @param idx Argument index.
     * @return Argument.
     */
    @SuppressWarnings("unchecked")
    public <T> T get(int idx) {
        return (T)args.get(idx);
    }

    /**
     * @return Size.
     */
    public int size() {
        return args.size();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeCollection(out, args);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        args = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopProtocolTaskArguments.class, this);
    }
}
