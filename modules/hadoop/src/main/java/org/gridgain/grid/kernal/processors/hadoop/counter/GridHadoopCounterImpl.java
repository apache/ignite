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
public class GridHadoopCounterImpl implements GridHadoopCounter, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Counter group name. */
    private String group;

    /** Counter name. */
    private String name;

    /** Counter current value. */
    private long val;

    /**
     * Default constructor required by {@link Externalizable}.
     */
    public GridHadoopCounterImpl() {
        // No-op.
    }

    /**
     * Creates new counter with given group and name.
     *
     * @param group Counter group name.
     * @param name Counter name.
     * @param val Counter value.
     */
    public GridHadoopCounterImpl(String group, String name, long val) {
        assert group != null : "counter must have group";
        assert name != null : "counter must have name";

        this.group = group;
        this.name = name;
        this.val = val;
    }

    /**
     * Copy constructor.
     *
     * @param cntr Counter to copy.
     */
    public GridHadoopCounterImpl(GridHadoopCounter cntr) {
        this(cntr.group(), cntr.name(), cntr.value());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override @Nullable public String group() {
        return group;
    }

    /** {@inheritDoc} */
    @Override public long value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public void value(long val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public void increment(long i) {
        val += i;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(group);
        out.writeUTF(name);
        out.writeLong(val);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        group = in.readUTF();
        name = in.readUTF();
        val = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GridHadoopCounterImpl counter = (GridHadoopCounterImpl)o;

        if (!group.equals(counter.group))
            return false;
        if (!name.equals(counter.name))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = group.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override public String toString() {
        return S.toString(GridHadoopCounterImpl.class, this);
    }
}
