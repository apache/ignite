/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import java.io.*;

/**
 * Abstract fragment of an input data source.
 */
public abstract class GridHadoopInputSplit implements Externalizable {
    /** */
    protected String[] hosts;

    /**
     * Array of hosts where this input split resides.
     *
     * @return Hosts.
     */
    public String[] hosts() {
        assert hosts != null;

        return hosts;
    }

    /**
     * This method must be implemented for purpose of internal implementation.
     *
     * @param obj Another object.
     * @return {@code true} If objects are equal.
     */
    @Override public abstract boolean equals(Object obj);

    /**
     * This method must be implemented for purpose of internal implementation.
     *
     * @return Hash code of the object.
     */
    @Override public abstract int hashCode();
}
