// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Atomic reference value.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridCacheAtomicReferenceValue<T> implements GridCacheInternal, GridPeerDeployAware,
    Externalizable, Cloneable {
    /** Value. */
    private T val;

    /**
     * Default constructor.
     *
     * @param val Initial value.
     */
    public GridCacheAtomicReferenceValue(T val) {
        this.val = val;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheAtomicReferenceValue() {
        // No-op.
    }

    /**
     * @param val New value.
     */
    public void set(T val) {
        this.val = val;
    }

    /**
     * @return val Current value.
     */
    public T get() {
        return val;
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked"})
    @Override public GridCacheAtomicReferenceValue<T> clone() throws CloneNotSupportedException {
        GridCacheAtomicReferenceValue<T> obj = (GridCacheAtomicReferenceValue<T>)super.clone();

        T locVal = X.cloneObject(val, false, true);

        obj.set(locVal);

        return obj;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        val = (T)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public Class<?> deployClass() {
        // First of all check classes that may be loaded by class loader other than application one.
        return val != null ? val.getClass() : getClass();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader classLoader() {
        return deployClass().getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAtomicReferenceValue.class, this);
    }
}
