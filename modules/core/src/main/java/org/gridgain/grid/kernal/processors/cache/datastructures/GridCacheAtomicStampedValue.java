/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;

/**
 * Atomic stamped value.
 */

public final class GridCacheAtomicStampedValue<T, S> implements GridCacheInternal, GridPeerDeployAware,
    Externalizable, Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value. */
    private T val;

    /** Stamp. */
    private S stamp;

    /**
     * Constructor.
     *
     * @param val Initial value.
     * @param stamp Initial stamp.
     */
    public GridCacheAtomicStampedValue(T val, S stamp) {
        this.val = val;
        this.stamp = stamp;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheAtomicStampedValue() {
        // No-op.
    }

    /**
     * @param val New value.
     * @param stamp New stamp.
     */
    public void set(T val, S stamp) {
        this.val = val;
        this.stamp = stamp;
    }

    /**
     * @return Current value and stamp.
     */
    public IgniteBiTuple<T, S> get() {
        return F.t(val, stamp);
    }

    /**
     * @return val Current value.
     */
    public T value() {
        return val;
    }

    /**
     * @return Current stamp.
     */
    public S stamp() {
        return stamp;
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked"})
    @Override public GridCacheAtomicStampedValue<T, S> clone() throws CloneNotSupportedException {
        GridCacheAtomicStampedValue<T, S> obj = (GridCacheAtomicStampedValue<T, S>)super.clone();

        T locVal = X.cloneObject(val, false, true);
        S locStamp = X.cloneObject(stamp, false, true);

        obj.set(locVal, locStamp);

        return obj;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(val);
        out.writeObject(stamp);
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        val = (T)in.readObject();
        stamp = (S)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public Class<?> deployClass() {
        ClassLoader clsLdr = getClass().getClassLoader();

        // First of all check classes that may be loaded by class loader other than application one.
        return stamp != null && !clsLdr.equals(stamp.getClass().getClassLoader()) ?
            stamp.getClass() : val != null ? val.getClass() : getClass();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader classLoader() {
        return deployClass().getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridCacheAtomicStampedValue.class, this);
    }
}
