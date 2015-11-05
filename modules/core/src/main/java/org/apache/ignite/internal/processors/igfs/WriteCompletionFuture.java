package org.apache.ignite.internal.processors.igfs;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsOutOfSpaceException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

/**
 * Future that is completed when all participating
 * parts of the file are written.
 */
public class WriteCompletionFuture extends GridFutureAdapter<Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Non-completed blocks count. It may both increase and decrease. */
    private final AtomicInteger awaitingAckBlocksCnt = new AtomicInteger();

    /** Flag indicating future is waiting for last ack. */
    private volatile boolean awaitingLast;

    /**
     */
    WriteCompletionFuture() {
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Boolean res, @Nullable Throwable err) {
        assert err != null || awaitingAckBlocksCnt.get() == 0;

        return super.onDone(res, err);
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

            if (afterAck == 0 && awaitingLast)
                onDone(true);
        }
    }

    /**
     * Marks this future as waiting the last ack.
     */
    void markWaitingLastAck() {
        awaitingLast = true;

        if (awaitingAckBlocksCnt.get() == 0)
            onDone(true);
    }
}