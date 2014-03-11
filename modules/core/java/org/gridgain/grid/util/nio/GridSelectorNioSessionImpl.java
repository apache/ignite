/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Session implementation bound to selector API and socket API.
 * Note that this implementation requires non-null values for local and remote
 * socket addresses.
 */
class GridSelectorNioSessionImpl extends GridNioSessionImpl {
    /** Pending write requests. */
    private final ConcurrentLinkedDeque8<GridNioFuture<?>> queue = new ConcurrentLinkedDeque8<>();

    /** Selection key associated with this session. */
    @GridToStringExclude
    private SelectionKey key;

    /** Worker index for server */
    private final int selectorIdx;

    /** Size counter. */
    private final AtomicInteger queueSize = new AtomicInteger();

    /** Semaphore. */
    @GridToStringExclude
    private final Semaphore sem;

    /** Write buffer. */
    private ByteBuffer writeBuf;

    /** Read buffer. */
    private ByteBuffer readBuf;

    /**
     * Creates session instance.
     *
     * @param selectorIdx Selector index for this session.
     * @param filterChain Filter chain that will handle requests.
     * @param locAddr Local address.
     * @param rmtAddr Remote address.
     * @param accepted Accepted flag.
     * @param sndQueueLimit Send queue limit.
     * @param writeBuf Write buffer.
     * @param readBuf Read buffer.
     */
    GridSelectorNioSessionImpl(
        int selectorIdx,
        GridNioFilterChain filterChain,
        InetSocketAddress locAddr,
        InetSocketAddress rmtAddr,
        boolean accepted,
        int sndQueueLimit,
        @Nullable ByteBuffer writeBuf,
        @Nullable ByteBuffer readBuf
    ) {
        super(filterChain, locAddr, rmtAddr, accepted);

        assert selectorIdx >= 0;
        assert sndQueueLimit >= 0;

        assert locAddr != null : "GridSelectorNioSessionImpl should have local socket address.";
        assert rmtAddr != null : "GridSelectorNioSessionImpl should have remote socket address.";

        this.selectorIdx = selectorIdx;

        sem = sndQueueLimit > 0 ? new Semaphore(sndQueueLimit) : null;

        if (writeBuf != null) {
            writeBuf.clear();

            this.writeBuf = writeBuf;
        }

        if (readBuf != null) {
            readBuf.clear();

            this.readBuf = readBuf;
        }
    }

    /**
     * Sets selection key for this session.
     *
     * @param key Selection key.
     */
    void key(SelectionKey key) {
        assert this.key == null;

        this.key = key;
    }

    /**
     * @return Write buffer.
     */
    public ByteBuffer writeBuffer() {
        return writeBuf;
    }

    /**
     * @return Read buffer.
     */
    public ByteBuffer readBuffer() {
        return readBuf;
    }

    /**
     * @return Registered selection key for this session.
     */
    SelectionKey key() {
        return key;
    }

    /**
     * @return Selector index.
     */
    int selectorIndex() {
        return selectorIdx;
    }

    /**
     * Adds write future to the pending list and returns the size of the queue.
     * <p>
     * Note that separate counter for the queue size is needed because in case of concurrent
     * calls this method should return different values (when queue size is 0 and 2 concurrent calls
     * occur exactly one call will return 1)
     *
     * @param writeFut Write request to add.
     * @return Updated size of the queue.
     */
    int offerFuture(GridNioFuture<?> writeFut) {
        boolean msgThread = GridNioBackPressureControl.threadProcessingMessage();

        if (sem != null && !msgThread)
            sem.acquireUninterruptibly();

        writeFut.messageThread(msgThread);

        boolean res = queue.offer(writeFut);

        assert res : "Future was not added to queue";

        return queueSize.incrementAndGet();
    }

    /**
     * @return Message that is in the head of the queue, {@code null} if queue is empty.
     */
    @Nullable GridNioFuture<?> pollFuture() {
        GridNioFuture<?> last = queue.poll();

        if (last != null) {
            queueSize.decrementAndGet();

            if (sem != null && !last.messageThread())
                sem.release();
        }

        return last;
    }

    /**
     * Gets number of write requests in a queue that have not been processed yet.
     *
     * @return Number of write requests.
     */
    int writeQueueSize() {
        return queueSize.get();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSelectorNioSessionImpl.class, this, super.toString());
    }
}
