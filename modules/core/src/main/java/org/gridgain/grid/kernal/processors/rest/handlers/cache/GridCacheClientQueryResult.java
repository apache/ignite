/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.cache;

import org.apache.ignite.portables.*;

import java.io.Serializable;
import java.util.*;

/**
 * Client query result.
 */
public class GridCacheClientQueryResult implements PortableMarshalAware, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query ID. */
    private long qryId;

    /** Result items. */
    private Collection<?> items;

    /** Last flag. */
    private boolean last;

    /** Node ID. */
    private UUID nodeId;

    /**
     * @return Query ID.
     */
    public long queryId() {
        return qryId;
    }

    /**
     * @param qryId Query ID.
     */
    public void queryId(long qryId) {
        this.qryId = qryId;
    }

    /**
     * @return Items.
     */
    public Collection<?> items() {
        return items;
    }

    /**
     * @param items Items.
     */
    public void items(Collection<?> items) {
        this.items = items;
    }

    /**
     * @return Last flag.
     */
    public boolean last() {
        return last;
    }

    /**
     * @param last Last flag.
     */
    public void last(boolean last) {
        this.last = last;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        PortableRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeBoolean(last);
        rawWriter.writeLong(qryId);
        rawWriter.writeUuid(nodeId);
        rawWriter.writeCollection(items);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        PortableRawReader rawReader = reader.rawReader();

        last = rawReader.readBoolean();
        qryId = rawReader.readLong();
        nodeId = rawReader.readUuid();
        items = rawReader.readCollection();
    }
}
