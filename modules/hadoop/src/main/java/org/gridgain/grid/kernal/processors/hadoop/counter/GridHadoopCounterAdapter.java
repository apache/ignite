/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.counter;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Default Hadoop counter implementation.
 */
public abstract class GridHadoopCounterAdapter<T> implements GridHadoopCounter<T>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Counter group name. */
    private String grp;

    /** Counter name. */
    private String name;

    /** Counter current value. */
    private T val;

    /**
     * Default constructor required by {@link Externalizable}.
     */
    protected GridHadoopCounterAdapter() {
        // No-op.
    }

    /**
     * Creates new counter with given group and name.
     *
     * @param grp Counter group name.
     * @param name Counter name.
     */
    protected GridHadoopCounterAdapter(String grp, String name) {
        assert grp != null : "counter must have group";
        assert name != null : "counter must have name";

        this.grp = grp;
        this.name = name;
    }

    /**
     * Copy constructor.
     *
     * @param cntr Counter to copy.
     */
    protected GridHadoopCounterAdapter(GridHadoopCounter cntr) {
        this(cntr.group(), cntr.name());

        value((T)cntr.value());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override @Nullable public String group() {
        return grp;
    }

    /** {@inheritDoc} */
    @Override public T value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public void value(T val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(grp);
        out.writeUTF(name);
        writeValue(out);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        grp = in.readUTF();
        name = in.readUTF();
        readValue(in);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GridHadoopCounterAdapter cntr = (GridHadoopCounterAdapter)o;

        if (!grp.equals(cntr.grp))
            return false;
        if (!name.equals(cntr.name))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = grp.hashCode();
        res = 31 * res + name.hashCode();
        return res;
    }

    @Override public String toString() {
        return S.toString(GridHadoopCounterAdapter.class, this);
    }

    protected abstract void writeValue(ObjectOutput out) throws IOException;

    protected abstract void readValue(ObjectInput in) throws IOException;

}
