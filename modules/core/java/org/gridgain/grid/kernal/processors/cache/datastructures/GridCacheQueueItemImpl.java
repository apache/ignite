/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Queue item. Instances of this class get stored in cache directly.
 * Note that fields annotated with {@link GridCacheQuerySqlField}
 * annotation will be indexed and can be used in cache queries. Also note that
 * arbitrary {@link #userObject()} put into the queue can also be indexed the same way.
 *
 * @author @java.author
 * @version @java.version
 */
@SuppressWarnings("AbbreviationUsage")
public class GridCacheQueueItemImpl<T> implements GridCacheQueueItem<T>, GridPeerDeployAware, Externalizable,
    Cloneable {
    /** User object. */
    private T obj;

    /** Queue item id. */
    @GridCacheQuerySqlField
    private int id;

    /** Queue id. */
    @GridCacheQuerySqlField
    private String qid;

    /** Sequence number. */
    @GridCacheQuerySqlField
    private long seq;

    /** Enqueue time. */
    private long enqueueTime;

    /** Dequeue time. */
    private long dequeueTime;

    /**
     * Constructs new item with specified id.
     *
     * @param seq Sequence id in queue.
     * @param qid Queue id.
     * @param obj User object being put into queue.
     */
    public GridCacheQueueItemImpl(String qid, long seq, T obj) {
        assert qid != null;
        assert obj != null;
        assert seq > 0;

        id = obj.hashCode();
        this.obj = obj;
        this.qid = qid;
        this.seq = seq;
    }

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheQueueItemImpl() {
        // No-op.
    }

    /**
     * Gets user object being put into queue.
     *
     * @return User object being put into queue.
     */
    @Override public T userObject() {
        return obj;
    }

    /**
     * Gets item id.
     *
     * @return Item id.
     */
    @Override public int id() {
        return id;
    }

    /**
     * Gets queue id.
     *
     * @return Item id.
     */
    @Override public String queueId() {
        return qid;
    }

    /**
     * Sets queue id.
     *
     * @param qid Queue id.
     */
    public void queueId(String qid) {
        this.qid = qid;
    }

    /**
     * Gets item enqueue time.
     *
     * @return Enqueue time.
     */
    public long enqueueTime() {
        return enqueueTime;
    }

    /**
     * Sets item enqueue time.
     *
     * @param enqueueTime Enqueue time.
     */
    public void enqueueTime(long enqueueTime) {
        this.enqueueTime = enqueueTime;
    }

    /**
     * Gets item dequeue time.
     *
     * @return Dequeue time.
     */
    public long dequeueTime() {
        return dequeueTime;
    }

    /**
     * Sets item dequeue time.
     *
     * @param dequeueTime Enqueue time.
     */
    public void dequeueTime(long dequeueTime) {
        this.dequeueTime = dequeueTime;
    }

    /**
     * Gets sequence number.
     *
     * @return Sequence number.
     */
    @Override public long sequence() {
        return seq;
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        GridCacheQueueItemImpl c = (GridCacheQueueItemImpl)super.clone();

        c.obj = X.cloneObject(obj, false, true);

        return c;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return qid.hashCode() + 31 * id + 31 * 31 * Long.valueOf(seq).hashCode();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(qid);
        out.writeLong(seq);
        out.writeLong(enqueueTime);
        out.writeLong(dequeueTime);
        out.writeObject(obj);
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readInt();
        qid = in.readUTF();
        seq = in.readLong();
        enqueueTime = in.readLong();
        dequeueTime = in.readLong();
        obj = (T)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public Class<?> deployClass() {
        return obj.getClass();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader classLoader() {
        return deployClass().getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj == this || (obj instanceof GridCacheQueueItemImpl && id == (((GridCacheQueueItem)obj).id())
            && seq == (((GridCacheQueueItemImpl)obj).seq) && qid.equals(((GridCacheQueueItemImpl)obj).qid));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueueItemImpl.class, this);
    }
}
