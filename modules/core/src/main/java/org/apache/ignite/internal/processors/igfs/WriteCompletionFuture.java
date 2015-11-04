package org.apache.ignite.internal.processors.igfs;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsOutOfSpaceException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Future that is completed when all participating
 * parts of the file are written.
 */
public class WriteCompletionFuture extends GridFutureAdapter<Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** File id to remove future from map. */
    private final IgniteUuid fileId;

    /** Non-completed blocks count. It may both increase and decrease. */
    private final AtomicInteger awaitingAckBlocksCnt = new AtomicInteger(0);

    /** Lock for map-related conditions. */
    private final Lock lock = new ReentrantLock();

    /** Condition to wait for empty map. */
    private final Condition allAcksRcvCond = lock.newCondition();

    /** Flag indicating future is waiting for last ack. */
    private volatile boolean awaitingLast;

    /**
     * @param fileId File id.
     */
    WriteCompletionFuture(IgniteUuid fileId) {
        assert fileId != null;

        this.fileId = fileId;
    }

    /**
     * Await all pending data blockes to be acked.
     *
     * @throws IgniteInterruptedCheckedException In case of interrupt.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void awaitAllAcksReceived() throws IgniteInterruptedCheckedException {
        lock.lock();

        try {
            while (!isDone() && awaitingAckBlocksCnt.get() > 0)
                U.await(allAcksRcvCond);
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Boolean res, @Nullable Throwable err) {
        assert err != null || awaitingAckBlocksCnt.get() == 0;

        boolean res1 = super.onDone(res, err);

        signalNoAcks();

        return res1;
    }

    /**
     * Write request will be asynchronously executed on node with given ID.
     */
    void onWriteRequest() {
        assert !awaitingLast; // Writing is finished, we should not receive more write requests.

        if (!isDone())
            awaitingAckBlocksCnt.incrementAndGet();
    }

    /**
     * Error occurred on node with given ID.
     *
     * @param e Caught exception.
     */
    void onError(IgniteCheckedException e) {
        if (e.hasCause(IgfsOutOfSpaceException.class))
            onDone(new IgniteCheckedException("Failed to write data (not enough space on node): ", e));
        else
            onDone(new IgniteCheckedException(
                "Failed to wait for write completion (write failed on node): ", e));
    }

    /**
     * Write ack received.
     */
    void onWriteAck() {
        if (!isDone()) {
            int afterAck = awaitingAckBlocksCnt.decrementAndGet();

            // This assertion is true because the number of counter decrements cannot exceed
            // the number of counter increments:
            assert afterAck >= 0 : "Received acknowledgement message for not registered batch.";

            if (afterAck == 0) {
                if (awaitingLast)
                    onDone(true);
                else
                    signalNoAcks();
            }
        }
    }

    /**
     * Signal that currenlty there are no more pending acks.
     */
    private void signalNoAcks() {
        lock.lock();

        try {
            allAcksRcvCond.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Marks this future as waiting last ack.
     */
    void markWaitingLastAck() {
        awaitingLast = true;

        if (awaitingAckBlocksCnt.get() == 0)
            onDone(true);
    }

    /**
     * Gets the file id.
     *
     * @return The file id.
     */
    public IgniteUuid getFileId() {
        return fileId;
    }
}
