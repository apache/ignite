// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Queue header.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheQueueHeader<T> implements GridCacheInternal, Externalizable, Cloneable {
    /** Queue id. */
    @GridCacheQuerySqlField
    private String qid;

    /** Queue type. */
    private GridCacheQueueType type;

    /** Maximum queue size. */
    private int cap;

    /** Actual queue size. */
    private int size;

    /** Sequence number. */
    private long seq;

    /** Collocation flag. */
    private boolean collocated;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheQueueHeader() {
        // No-op.
    }

    /**
     * Default constructor.
     *
     * @param qid Name of queue.
     * @param type Type of queue.
     * @param cap Capacity of queue.
     * @param collocated Collocation flag.
     */
    public GridCacheQueueHeader(String qid, GridCacheQueueType type, int cap, boolean collocated) {
        assert qid != null;
        assert type != null;
        assert cap > 0;

        this.cap = cap;
        this.type = type;
        this.qid = qid;
        this.collocated = collocated;
    }

    /**
     * Gets type of queue.
     *
     * @return Type of queue.
     */
    public GridCacheQueueType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(type.ordinal());
        out.writeInt(cap);
        out.writeInt(size);
        out.writeLong(seq);
        out.writeBoolean(collocated);
        out.writeUTF(qid);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type = GridCacheQueueType.fromOrdinal(in.readInt());
        cap = in.readInt();
        size = in.readInt();
        seq = in.readLong();
        collocated = in.readBoolean();
        qid = in.readUTF();
    }

    /**
     * @return Sequence number.
     */
    public long sequence() {
        return seq;
    }

    /**
     * @return Incremented sequence number.
     */
    public long incrementSequence() {
        return ++seq;
    }

    /**
     * @return Maximum queue size.
     */
    public int capacity() {
        return cap;
    }

    /**
     * @return Collocation flag.
     */
    public boolean collocated() {
        return collocated;
    }

    /**
     * Gets actual queue size.
     *
     * @return Actual queue size.
     */
    public int size() {
        return size;
    }

    /**
     * Sets actual queue size.
     *
     * @param size Actual queue size.
     */
    public void size(int size) {
        this.size = size;
    }

    /**
     * Increments queue size.
     */
    public void incrementSize() {
        size++;
    }

    /**
     * Decrements queue size.
     */
    public void decrementSize() {
        assert size > 0;

        size--;
    }

    /**
     * Checks whether queue is full.
     *
     * @return {@code true} if queue is full.
     */
    public boolean full() {
        return cap > 0 && size == cap;
    }

    /**
     * Checks whether queue is empty.
     *
     * @return {@code true} if queue is empty.
     */
    public boolean empty() {
        return size == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof GridCacheQueueHeader))
            return false;

        GridCacheQueueHeader hdr = (GridCacheQueueHeader)obj;

        return qid.equals(hdr.qid) && type == hdr.type;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = qid.hashCode();

        result = 31 * result + type.hashCode();

        return result;
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueueHeader.class, this);
    }
}
