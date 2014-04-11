/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.gridgain.grid.*;

import java.io.*;
import java.util.*;

/**
 * TODO write doc
 */
public class GridHadoopBlock implements Externalizable {
    /** Path. */
    private String path;

    /** Offset. */
    private long offset;

    /** Length. */
    private long len;

    /** Hosts. */
    private String[] hosts;

    /**
     * @param path Block's file path.
     * @param offset Block start offset.
     * @param len Block length.
     */
    public GridHadoopBlock(String path, long offset, long len) {
        this.path = path;
        this.offset = offset;
        this.len = len;
    }

    public void locations(String[] locations) {
        hosts = locations;
    }

    public long length() {
        return 0;
    }

    public Iterator<GridNode> ownerNodes(Collection<GridNode> top) {
        return null;
    }

    public Iterator<GridNode> hostNodes(Collection<GridNode> top) {
        return null;
    }

    public Iterator<GridNode> rackNodes(Collection<GridNode> top) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {

    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }
}
