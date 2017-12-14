package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import org.apache.ignite.internal.util.future.CountDownFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Count down future, but allows to increment waiting tasks count.
 */
class CountDownDynamicFuture extends CountDownFuture {
    private volatile boolean completionObserved = false;

    /**
     * @param cnt Number of completing parties.
     */
    CountDownDynamicFuture(int cnt) {
        super(cnt);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        final boolean b = super.onDone(res, err);

        completionObserved |= b;

        return b;
    }

    /**
     * Grows counter of submitted tasks to be waited for complete.
     *
     * Call this method only if counter can't become 0.
     */
    void incrementTasksCount() {
        if (completionObserved)
            throw new IllegalStateException("Future already completed, not allowed to submit new tasks");

        remaining().incrementAndGet();
    }
}
