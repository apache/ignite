package org.apache.ignite.internal.processors.cache.query;

import java.util.NoSuchElementException;
import org.apache.ignite.IgniteCheckedException;

/** */
public class LimitedReducer<T> implements Reducer<T> {
    /** */
    private final Reducer<T> delegate;

    /** */
    private final int limit;

    /** TODO: thread safety? */
    private int cnt;

    /** */
    public LimitedReducer(Reducer<T> delegate, int limit) {
        this.delegate = delegate;
        this.limit = limit;
    }

    /** {@inheritDoc} */
    @Override public void addPage(CacheQueryResultPage<T> page) {
        delegate.addPage(page);
    }

    /** {@inheritDoc} */
    @Override public void onLastPage() {
        delegate.onLastPage();
    }

    /** {@inheritDoc} */
    @Override public T next() throws IgniteCheckedException {
        if (!checkLimit())
            throw new NoSuchElementException();

        cnt += 1;

        return delegate.next();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() throws IgniteCheckedException {
        if (!checkLimit())
            return false;

        return delegate.hasNext();
    }

    /** {@inheritDoc} */
    @Override public Object sharedLock() {
        return delegate.sharedLock();
    }

    /** */
    private boolean checkLimit() {
        return cnt < limit;
    }
}
