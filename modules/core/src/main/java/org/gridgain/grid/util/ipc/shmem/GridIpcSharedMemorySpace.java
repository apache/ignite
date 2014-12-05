/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.IgniteSystemProperties.*;

/**
 *
 */
@SuppressWarnings({"PointlessBooleanExpression", "ConstantConditions"})
public class GridIpcSharedMemorySpace implements Closeable {
    /** Debug flag (enable for testing). */
    private static final boolean DEBUG = Boolean.getBoolean(GG_IPC_SHMEM_SPACE_DEBUG);

    /** Shared memory segment size (operable). */
    private final int opSize;

    /** Shared memory native pointer. */
    private final long shmemPtr;

    /** Shared memory ID. */
    private final int shmemId;

    /** Semaphore set ID. */
    private final int semId;

    /** Local closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** {@code True} if space has been closed. */
    private final boolean isReader;

    /** Lock to protect readers and writers from concurrent close. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
    private final int writerPid;

    /** */
    private final int readerPid;

    /** */
    private final String tokFileName;

    /** */
    private final IgniteLogger log;

    /**
     * This will allocate system resources for the space.
     *
     * @param tokFileName Token filename.
     * @param writerPid Writer PID.
     * @param readerPid Reader PID.
     * @param size Size in bytes.
     * @param reader {@code True} if reader.
     * @param parent Parent logger.
     * @throws GridException If failed.
     */
    public GridIpcSharedMemorySpace(String tokFileName, int writerPid, int readerPid, int size, boolean reader,
        IgniteLogger parent) throws GridException {
        assert size > 0 : "Size cannot be less than 1 byte";

        log = parent.getLogger(GridIpcSharedMemorySpace.class);

        opSize = size;

        shmemPtr = GridIpcSharedMemoryUtils.allocateSystemResources(tokFileName, size, DEBUG && log.isDebugEnabled());

        shmemId = GridIpcSharedMemoryUtils.sharedMemoryId(shmemPtr);
        semId = GridIpcSharedMemoryUtils.semaphoreId(shmemPtr);

        isReader = reader;

        this.tokFileName = tokFileName;
        this.readerPid = readerPid;
        this.writerPid = writerPid;

        if (DEBUG && log.isDebugEnabled())
            log.debug("Shared memory space has been created: " + this);
    }

    /**
     * This should be called in order to attach to already allocated system resources.
     *
     * @param tokFileName Token file name (for proper cleanup).
     * @param writerPid Writer PID.
     * @param readerPid Reader PID.
     * @param size Size.
     * @param reader Reader flag.
     * @param shmemId Shared memory ID.
     * @param parent Logger.
     * @throws GridException If failed.
     */
    public GridIpcSharedMemorySpace(String tokFileName, int writerPid, int readerPid, int size, boolean reader,
        int shmemId, IgniteLogger parent) throws GridException {
        assert size > 0 : "Size cannot be less than 1 byte";

        log = parent.getLogger(GridIpcSharedMemorySpace.class);

        opSize = size;
        isReader = reader;
        this.shmemId = shmemId;
        this.writerPid = writerPid;
        this.readerPid = readerPid;
        this.tokFileName = tokFileName;

        shmemPtr = GridIpcSharedMemoryUtils.attach(shmemId, DEBUG && log.isDebugEnabled());

        semId = GridIpcSharedMemoryUtils.semaphoreId(shmemPtr);
    }

    /**
     * @param buf Buffer.
     * @param off Offset.
     * @param len Length.
     * @param timeout Operation timeout in milliseconds ({@code 0} to wait forever).
     * @throws GridException If space has been closed.
     * @throws GridIpcSharedMemoryOperationTimedoutException If operation times out.
     */
    public void write(byte[] buf, int off, int len, long timeout) throws GridException,
        GridIpcSharedMemoryOperationTimedoutException {
        assert buf != null;
        assert len > 0;
        assert buf.length >= off + len;
        assert timeout >= 0;

        assert !isReader;

        lock.readLock().lock();

        try {
            if (closed.get())
                throw new GridException("Shared memory segment has been closed: " + this);

            GridIpcSharedMemoryUtils.writeSharedMemory(shmemPtr, buf, off, len, timeout);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param buf Buffer.
     * @param off Offset.
     * @param len Length.
     * @param timeout Operation timeout in milliseconds ({@code 0} to wait forever).
     * @throws GridException If space has been closed.
     * @throws GridIpcSharedMemoryOperationTimedoutException If operation times out.
     */
    public void write(ByteBuffer buf, int off, int len, long timeout) throws GridException,
        GridIpcSharedMemoryOperationTimedoutException {
        assert buf != null;
        assert len > 0;
        assert buf.limit() >= off + len;
        assert timeout >= 0;
        assert !isReader;

        lock.readLock().lock();

        try {
            if (closed.get())
                throw new GridException("Shared memory segment has been closed: " + this);

            GridIpcSharedMemoryUtils.writeSharedMemoryByteBuffer(shmemPtr, buf, off, len, timeout);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Blocks until at least 1 byte is read.
     *
     * @param buf Buffer.
     * @param off Offset.
     * @param len Length.
     * @param timeout Operation timeout in milliseconds ({@code 0} to wait forever).
     * @return Read bytes count.
     * @throws GridException If space has been closed.
     * @throws GridIpcSharedMemoryOperationTimedoutException If operation times out.
     */
    public int read(byte[] buf, int off, int len, long timeout) throws GridException,
        GridIpcSharedMemoryOperationTimedoutException{
        assert buf != null;
        assert len > 0;
        assert buf.length >= off + len;

        assert isReader;

        lock.readLock().lock();

        try {
            if (closed.get())
                throw new GridException("Shared memory segment has been closed: " + this);

            return (int) GridIpcSharedMemoryUtils.readSharedMemory(shmemPtr, buf, off, len, timeout);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Blocks until at least 1 byte is read.
     *
     * @param buf Buffer.
     * @param off Offset.
     * @param len Length.
     * @param timeout Operation timeout in milliseconds ({@code 0} to wait forever).
     * @return Read bytes count.
     * @throws GridException If space has been closed.
     * @throws GridIpcSharedMemoryOperationTimedoutException If operation times out.
     */
    public int read(ByteBuffer buf, int off, int len, long timeout) throws GridException,
        GridIpcSharedMemoryOperationTimedoutException{
        assert buf != null;
        assert len > 0;
        assert buf.capacity() >= off + len;
        assert isReader;

        lock.readLock().lock();

        try {
            if (closed.get())
                throw new GridException("Shared memory segment has been closed: " + this);

            return (int) GridIpcSharedMemoryUtils.readSharedMemoryByteBuffer(shmemPtr, buf, off, len, timeout);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        close0(false);
    }

    /**
     * Forcibly closes the space and frees all system resources.
     * <p>
     * This method should be called with caution as it may result to the other-party
     * process crash. It is intended to call when there was an IO error during handshake
     * and other party has not yet attached to the space.
     */
    public void forceClose() {
        close0(true);
    }

    /**
     * @return Shared memory ID.
     */
    public int sharedMemoryId() {
        return shmemId;
    }

    /**
     * @return Semaphore set ID.
     */
    public int semaphoreId() {
        return semId;
    }

    /**
     * @param force {@code True} to close the space.
     */
    private void close0(boolean force) {
        if (!closed.compareAndSet(false, true))
            return;

        GridIpcSharedMemoryUtils.ipcClose(shmemPtr);

        // Wait all readers and writes to leave critical section.
        lock.writeLock().lock();

        try {
            GridIpcSharedMemoryUtils.freeSystemResources(tokFileName, shmemPtr, force);
        }
        finally {
            lock.writeLock().unlock();
        }

        if (DEBUG && log.isDebugEnabled())
            log.debug("Shared memory space has been closed: " + this);
    }

    /**
     * @return Bytes available for read.
     * @throws GridException If failed.
     */
    public int unreadCount() throws GridException {
        lock.readLock().lock();

        try {
            if (closed.get())
                throw new GridException("Shared memory segment has been closed: " + this);

            return GridIpcSharedMemoryUtils.unreadCount(shmemPtr);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @return Shared memory pointer.
     */
    public long sharedMemPointer() {
        return shmemPtr;
    }

    /**
     * @return Reader PID.
     */
    public int readerPid() {
        return readerPid;
    }

    /**
     * @return Writer PID.
     */
    public int writerPid() {
        return writerPid;
    }

    /**
     * @return Vis-a-vis PID.
     */
    public int otherPartyPid() {
        return isReader ? writerPid : readerPid;
    }

    /**
     * @return Token file name used to create shared memory space.
     */
    public String tokenFileName() {
        return tokFileName;
    }

    /**
     * @return Space size.
     */
    public int size() {
        return opSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIpcSharedMemorySpace.class, this, "closed", closed.get());
    }
}
