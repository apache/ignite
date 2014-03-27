/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * {@link GridCacheQueue} implementation using atomic cache.
 */
public class GridAtomicCacheQueueImpl<T> extends GridCacheQueueAdapter<T> {
    /** */
    private static final long RETRY_TIMEOUT = 2000;

    /**
     * @param queueName Queue name.
     * @param uuid Queue UUID.
     * @param cap Capacity.
     * @param collocated Collocation flag.
     * @param cctx Cache context.
     */
    public GridAtomicCacheQueueImpl(String queueName, GridUuid uuid, int cap, boolean collocated,
        GridCacheContext<?, ?> cctx) {
        super(queueName, uuid, cap, collocated, cctx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean offer(T item) throws GridRuntimeException {
        try {
            Long idx = (Long)cache.transformCompute(queueKey, new AddClosure(uuid, 1));

            if (idx == null)
                return false;

            checkRemoved(idx);

            cache.putx(new ItemKey(uuid, idx, collocated()), item, null);

            return true;
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public T poll() throws GridRuntimeException {
        try {
            Long idx = (Long)cache.transformCompute(queueKey, new PollClosure(uuid));

            if (idx == null)
                return null;

            checkRemoved(idx);

            ItemKey key = new ItemKey(uuid, idx, collocated());

            T data = (T)cache.remove(key, null);

            if (data != null)
                return data;

            long stop = U.currentTimeMillis() + RETRY_TIMEOUT;

            while (U.currentTimeMillis() < stop ) {
                data = (T)cache.remove(key, null);

                if (data != null)
                    return data;
            }

            U.warn(log, "Failed to get item, retrying poll [queue=" + queueName + ", idx=" + idx + ']');

            return poll();
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void removeItem(long rmvIdx) throws GridException {
        Long idx = (Long)cache.transformCompute(queueKey, new RemoveClosure(uuid, rmvIdx));

        if (idx != null) {
            checkRemoved(idx);

            ItemKey key = new ItemKey(uuid, idx, collocated());

            if (cache.removex(key))
                return;

            long stop = U.currentTimeMillis() + RETRY_TIMEOUT;

            while (U.currentTimeMillis() < stop ) {
                if (cache.removex(key))
                    return;
            }

            U.warn(log, "Failed to remove item, [queue=" + queueName + ", idx=" + idx + ']');
        }
    }
}
