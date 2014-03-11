/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 *
 */
public class GridIpcSharedMemoryInitResponse implements Externalizable {
    /** */
    private String inTokFileName;

    /** */
    private int inSharedMemId;

    /** */
    private String outTokFileName;

    /** */
    private int outSharedMemId;

    /** */
    private int pid;

    /** */
    private int size;

    /** */
    private Exception err;

    /**
     * Constructs a successful response.
     *
     * @param inTokFileName In token.
     * @param inSharedMemId In  shared memory ID.
     * @param outTokFileName Out token.
     * @param outSharedMemId Out shared memory ID.
     * @param pid PID of the {@code server} party.
     * @param size Size.
     */
    public GridIpcSharedMemoryInitResponse(String inTokFileName, int inSharedMemId, String outTokFileName,
        int outSharedMemId, int pid, int size) {
        this.inTokFileName = inTokFileName;
        this.inSharedMemId = inSharedMemId;
        this.outTokFileName = outTokFileName;
        this.outSharedMemId = outSharedMemId;
        this.pid = pid;
        this.size = size;
    }

    /**
     * Constructs an error response.
     *
     * @param err Error cause.
     */
    public GridIpcSharedMemoryInitResponse(Exception err) {
        this.err = err;
    }

    /**
     * Required by {@code Externalizable}.
     */
    public GridIpcSharedMemoryInitResponse() {
        // No-op.
    }

    /**
     * @return In token file name or {@code null}, if this is an error response.
     */
    @Nullable public String inTokenFileName() {
        return inTokFileName;
    }

    /**
     * @return In shared memory ID.
     */
    public int inSharedMemoryId() {
        return inSharedMemId;
    }

    /**
     * @return Out token file name or {@code null}, if this is an error response.
     */
    @Nullable public String outTokenFileName() {
        return outTokFileName;
    }

    /**
     * @return Out shared memory ID.
     */
    public int outSharedMemoryId() {
        return outSharedMemId;
    }

    /**
     * @return Sender PID.
     */
    public int pid() {
        return pid;
    }

    /**
     * @return Space size.
     */
    public int size() {
        return size;
    }

    /**
     * @return Error message or {@code null}, if this is
     *         a successful response.
     */
    @Nullable public Exception error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, inTokFileName);
        out.writeInt(inSharedMemId);
        U.writeString(out, outTokFileName);
        out.writeInt(outSharedMemId);
        out.writeObject(err);
        out.writeInt(pid);
        out.writeInt(size);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        inTokFileName = U.readString(in);
        inSharedMemId = in.readInt();
        outTokFileName = U.readString(in);
        outSharedMemId = in.readInt();
        err = (Exception)in.readObject();
        pid = in.readInt();
        size = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridIpcSharedMemoryInitResponse [err=" + err +
            ", inTokFileName=" + inTokFileName +
            ", inSharedMemId=" + inSharedMemId +
            ", outTokFileName=" + outTokFileName +
            ", outSharedMemId=" + outSharedMemId +
            ", pid=" + pid +
            ", size=" + size + ']';
    }
}
