/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * TODO
 */
public class GridCacheQueueHeader2 implements Externalizable {
    /** */
    private GridUuid uuid;

    /** */
    private long head;

    /** */
    private long tail;

    public GridCacheQueueHeader2() {
    }

    public GridCacheQueueHeader2(GridUuid uuid, long head, long tail) {
        this.uuid = uuid;
        this.head = head;
        this.tail = tail;
    }

    public GridUuid uuid() {
        return uuid;
    }

    public long head() {
        return head;
    }

    public long tail() {
        return tail;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, uuid);
        out.writeLong(head);
        out.writeLong(tail);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        uuid = U.readGridUuid(in);
        head = in.readLong();
        tail = in.readLong();
    }
}
