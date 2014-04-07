/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.marshaller;

import org.gridgain.grid.util.typedef.internal.*;
import java.io.*;

/**
 * Externalizable bean for marshaller testing.
 */
class GridMarshallerExternalizableBean implements Externalizable {
    /** */
    private int i;

    /**
     * Required for {@link Externalizable}.
     */
    public GridMarshallerExternalizableBean() {
        // No=op.
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(i);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        i = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        assert o instanceof GridMarshallerExternalizableBean;

        return i == ((GridMarshallerExternalizableBean)o).i;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return i;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridMarshallerExternalizableBean.class, this);
    }
}
