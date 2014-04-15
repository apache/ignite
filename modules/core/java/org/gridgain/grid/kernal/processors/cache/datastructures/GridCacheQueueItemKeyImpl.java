/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Represents cache key for {@link GridCacheQueueItem}. This class is used by implementation
 * and is not known to public API. It is responsible for data affinity of a queued item
 * when used in {@code PARTITIONED} cache mode. In particular, all items belonging to the
 * same queue will be stored on the same node or distributed through grid nodes -
 * hence the {@link GridCacheAffinityKeyMapped} annotation on {@link GridCacheQueueItemKeyImpl#affinityKey} method.
 */
public class GridCacheQueueItemKeyImpl implements Externalizable, GridCacheQueueItemKey {
    private static final long serialVersionUID = 1613357332187143299L;
    /** Sequence id in queue. */
    private long seq;

    /** Queue id. */
    private String qid;

    /** Collocated flag. */
    private boolean colloc;

    /**
     * Constructs queue item cache key.
     *
     * @param seq Queue item id.
     * @param qid Queue id.
     * @param colloc If {@code true} all queue items must be saved on one node.
     */
    public GridCacheQueueItemKeyImpl(long seq, String qid, boolean colloc) {
        assert seq >= 0;
        assert qid != null;

        this.seq = seq;
        this.qid = qid;
        this.colloc = colloc;
    }

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheQueueItemKeyImpl() {
        // No-op.
    }

    /**
     * Gets queue item sequence id.
     *
     * @return Queue item sequence id.
     */
    @Override public long sequence() {
        return seq;
    }

    /**
     * Gets queue id.
     *
     * @return Queue id.
     */
    @Override public String queueId() {
        return qid;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * ((int)(seq ^ (seq >>> 32))) + qid.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj == this || obj instanceof GridCacheQueueItemKeyImpl &&
            seq == (((GridCacheQueueItemKey) obj).sequence()) && qid.equals(((GridCacheQueueItemKey) obj).queueId());
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(seq);
        out.writeUTF(qid);
        out.writeBoolean(colloc);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        seq = in.readLong();
        qid = in.readUTF();
        colloc = in.readBoolean();
    }

    /** {@inheritDoc} */
    @GridCacheAffinityKeyMapped
    @Override public String affinityKey() {
        return colloc ? qid : qid + "_" + seq;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueueItemKeyImpl.class, this);
    }
}
