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
import java.util.concurrent.atomic.*;

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
    public AtomicLong value = new AtomicLong(0);

    /**
     * Default constructor required by {@link Externalizable}.
     */
    public GridHadoopCounterImpl() {
    }

    /**
     * Creates new counter without group assignment.
     *
     * @param name Counter name.
     */
    public GridHadoopCounterImpl(String name) {
        this.name = name;
    }

    /**
     * Creates new counter with given group and name.
     *
     * @param group Counter group name.
     * @param name Counter name.
     */
    public GridHadoopCounterImpl(@Nullable String group, String name) {
        if (name == null) {
            throw new IllegalArgumentException("counter must have name");
        }

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
        return value.get();
    }

    /** {@inheritDoc} */
    @Override public void value(long val) {
        value.set(val);
    }

    /** {@inheritDoc} */
    @Override public void increment(long i) {
        value.addAndGet(i);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(group);
        out.writeUTF(name);
        out.writeLong(value.get());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        group = (String)in.readObject();
        name = in.readUTF();
        value = new AtomicLong(in.readLong());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GridHadoopCounterImpl counter = (GridHadoopCounterImpl)o;

        if (group != null ? !group.equals(counter.group) : counter.group != null) {
            return false;
        }
        if (!name.equals(counter.name)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = group != null ? group.hashCode() : 0;
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override public String toString() {
        return S.toString(GridHadoopCounterImpl.class, this);
    }

}
