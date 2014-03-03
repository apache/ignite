/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem;

import java.io.*;

/**
 * @author @java.author
 * @version @java.version
 */
public class GridIpcSharedMemoryInitRequest implements Externalizable {
    /** */
    private int pid;

    /**
     * @param pid PID of the {@code client} party.
     */
    public GridIpcSharedMemoryInitRequest(int pid) {
        this.pid = pid;
    }

    /**
     * Required by {@code Externalizable}.
     */
    public GridIpcSharedMemoryInitRequest() {
        // No-op.
    }

    /**
     * @return Sender PID.
     */
    public int pid() {
        return pid;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(pid);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        pid = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridIpcSharedMemoryInitRequest [pid=" + pid + ']';
    }
}
