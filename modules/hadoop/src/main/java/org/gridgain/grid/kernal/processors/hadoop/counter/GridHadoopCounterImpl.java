// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.counter;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Default Hadoop counter implementation.
 */
public class GridHadoopCounterImpl implements GridHadoopCounter, Externalizable {
    /** Counter group name. */
    @GridToStringInclude
    public String group;

    /** Counter name. */
    @GridToStringInclude
    public String name;

    /** Counter current value. */
    @GridToStringInclude
    public long value = 0;

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
     */
    public GridHadoopCounterImpl(String group, String name) {
        assert group != null : "counter must have group";
        assert name != null : "counter must have name";

        this.group = group;
        this.name = name;
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
        return value;
    }

    /** {@inheritDoc} */
    @Override public void value(long val) {
        value = val;
    }

    /** {@inheritDoc} */
    @Override public void increment(long i) {
        value += i;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(group);
        out.writeUTF(name);
        out.writeLong(value);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        group = (String)in.readObject();
        name = in.readUTF();
        value = in.readLong();
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
