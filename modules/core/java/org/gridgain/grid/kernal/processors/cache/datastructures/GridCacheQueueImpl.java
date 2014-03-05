/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.kernal.processors.cache.datastructures.GridCacheQueueOperation.*;
import static org.gridgain.grid.kernal.processors.cache.datastructures.GridCacheQueueQueryFactory.*;

/**
 * Queue implementation.
 */
public class GridCacheQueueImpl<T> extends AbstractCollection<T> implements GridCacheQueueEx<T>,
    Externalizable {
    /** Deserialization stash. */
    private static final ThreadLocal<GridBiTuple<GridCacheContext, String>> stash =
        new ThreadLocal<GridBiTuple<GridCacheContext, String>>() {
            @Override protected GridBiTuple<GridCacheContext, String> initialValue() {
                return F.t2();
            }
        };

    /** Default value of warning threshold of attempts. */
    public static final int DFLT_ATTEMPT_WARN_THRESHOLD = 5;

    /** Logger. */
    private GridLogger log;

    /** Queue id. */
    private String qid;

    /** Removed flag. */
    private volatile boolean rmvd;

    /** Bounded flag. */
    private volatile boolean bounded;

    /** Maximum queue size. */
    private int cap;

    /** Collocation flag. */
    private boolean collocated;

    /** Queue key. */
    private GridCacheInternalKey key;

    /** Read blocking operations semaphore. */
    @GridToStringExclude
    private volatile Semaphore readSem;

    /** Write blocking operations semaphore. */
    @GridToStringExclude
    private volatile Semaphore writeSem;

    /** Warning threshold in blocking operations.*/
    @GridToStringExclude
    private long blockAttemptWarnThreshold = DFLT_ATTEMPT_WARN_THRESHOLD;

    /** Cache context. */
    private GridCacheContext cctx;

    /** Queue header view. */
    @GridToStringExclude
    private GridCacheProjection<GridCacheInternalKey, GridCacheQueueHeader> queueHdrView;

    /** Queue item view. */
    @GridToStringExclude
    private GridCacheProjection<GridCacheQueueItemKey, GridCacheQueueItem<T>> itemView;

    /** Query factory. */
    @GridToStringExclude
    private GridCacheQueueQueryFactory<T> qryFactory;

    /** Mutex. */
    @GridToStringExclude
    private final Object mux = new Object();

    /** Internal error state of the queue. */
    @GridToStringInclude
    private volatile Exception err;

    /**
     * Constructor.
     *
     * @param qid Query Id.
     * @param hdr  Header of queue.
     * @param key Key of queue.
     * @param cctx Cache context.
     * @param queueHdrView Queue headers view.
     * @param itemView  Queue items view.
     * @param qryFactory Query factory.
     */
    public GridCacheQueueImpl(String qid, GridCacheQueueHeader hdr, GridCacheInternalKey key,
        GridCacheContext<?, ?> cctx, GridCacheProjection<GridCacheInternalKey,
        GridCacheQueueHeader> queueHdrView,
        GridCacheProjection<GridCacheQueueItemKey, GridCacheQueueItem<T>> itemView,
        GridCacheQueueQueryFactory<T> qryFactory) {
        assert qid != null;
        assert hdr != null;
        assert key != null;
        assert cctx != null;
        assert queueHdrView != null;
        assert itemView != null;
        assert qryFactory != null;

        this.qid = qid;
        this.key = key;
        this.cctx = cctx;
        this.queueHdrView = queueHdrView;
        this.itemView = itemView;
        this.qryFactory = qryFactory;

        readSem = new Semaphore(hdr.size(), true);

        writeSem = new Semaphore(hdr.capacity() - hdr.size(), true);

        cap = hdr.capacity();

        collocated = hdr.collocated();

        bounded = cap < Integer.MAX_VALUE;

        log = cctx.logger(GridCacheQueueImpl.class);
    }

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridCacheQueueImpl() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return qid;
    }

    /** {@inheritDoc} */
    @Override public boolean offer(T item) {
        A.notNull(item, "item");

        checkRemoved();

        try {
            return CU.outTx(addCallable(Arrays.asList(item)), cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean offer(T item, long timeout, TimeUnit unit) {
        A.notNull(item, "item");
        A.ensure(timeout >= 0, "Timeout cannot be negative: " + timeout);

        try {
            checkRemovedx();

            return bounded ? blockWriteOp(Arrays.asList(item), PUT_TIMEOUT, timeout, unit) :
                CU.outTx(addCallable(Arrays.asList(item)), cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean add(T item) {
        A.notNull(item, "item");

        checkRemoved();

        try {
            boolean retVal = CU.outTx(addCallable(Arrays.asList(item)), cctx);

            if (!retVal)
                throw new IllegalStateException();

            return true;
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(Collection<? extends T> items) {
        A.notNull(items, "items");

        checkRemoved();

        try {
            return CU.outTx(addCallable((Collection<T>)items), cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        A.notNull(o, "o");

        T item = (T)o;

        T[] items = (T[])new Object[] {item};

        Object[] hashes = new Object[] {item.hashCode()};

        try {
            return internalContains(items, hashes);
        }
        catch (GridException e) {
            throw new GridRuntimeException("Failed to find queue item [queue=" + qid + ", item=" + item + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(Collection<?> items) {
        A.ensure(!F.isEmpty(items), "items cannot be empty");

        // Try to cast collection.
        Collection<T> items0 = (Collection<T>)items;

        // Prepare id's for query.
        Collection<Integer> hashes = new LinkedList<>();

        for (T item : items0)
            hashes.add(item.hashCode());

        try {
            return internalContains((T[])items0.toArray(), hashes.toArray());
        }
        catch (GridException e) {
            throw new GridRuntimeException("Failed to find queue items " +
                "[queue=" + qid + ", items=" + items0 + ']', e);
        }
    }

    /**
     * @param hashes Array of items hash code
     * @param items Array of items.
     * @return {@code True} if queue contains all items, {@code false} otherwise.
     * @throws GridException If operation failed.
     */
    private boolean internalContains(T[] items, Object[] hashes) throws GridException {
        GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> qry =
            qryFactory.containsQuery();

        if (collocated) {
            // For case if primary node was changed during request.
            GridNode node = CU.primaryNode(cctx, key);

            qry.projection(cctx.grid().forNode(node));
        }

        ContainsQueryLocalReducer locRdc = new ContainsQueryLocalReducer(items.length);

        return F.reduce(qry.execute(new ContainsQueryRemoteReducer<T>(items), qid, hashes).get(), locRdc);
    }

    /** {@inheritDoc} */
    @Override public T poll() {
        try {
            checkRemovedx();

            return CU.outTx(queryCallable(qryFactory.firstItemQuery(), new TerminalItemQueryLocalReducer<T>(true),
                false), cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public T remove() {
        T res = poll();

        if (res == null)
            throw new NoSuchElementException();

        return res;
    }

    /** {@inheritDoc} */
    @Override public T poll(long timeout, TimeUnit unit) {
        A.ensure(timeout >= 0, "Timeout cannot be negative: " + timeout);

        try {
            checkRemovedx();

            boolean peek = false;

            return blockReadOp(queryCallable(qryFactory.firstItemQuery(), new TerminalItemQueryLocalReducer<T>(true),
                peek), TAKE_TIMEOUT, timeout, unit, peek);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public T element() {
        T el = peek();

        if (el == null)
            throw new NoSuchElementException();

        return el;
    }

    /** {@inheritDoc} */
    @Override public T peek() {
        try {
            checkRemovedx();

            return CU.outTx(queryCallable(qryFactory.firstItemQuery(), new TerminalItemQueryLocalReducer<T>(true),
                true), cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object item) {
        A.notNull(item, "item");

        T locItem = (T)item;

        checkRemoved();

        try {
            return CU.outTx(removeItemsCallable(Arrays.asList(locItem), false), cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(Collection<?> items) {
        A.ensure(!F.isEmpty(items), "items cannot be empty");

        // Try to cast collection.
        Collection<T> items0 = (Collection<T>)items;

        checkRemoved();

        try {
            return CU.outTx(removeItemsCallable(items0, false), cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(Collection<?> items) {
        A.ensure(!F.isEmpty(items), "items cannot be empty");

        // Try to cast collection.
        Collection<T> items0 = (Collection<T>)items;

        checkRemoved();

        try {
            return CU.outTx(removeItemsCallable(items0, true), cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(T item) {
        A.notNull(item, "item");

        try {
            checkRemovedx();

            if (bounded)
                blockWriteOp(Arrays.asList(item), PUT);
            else
                CU.outTx(addCallable(Arrays.asList(item)), cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public T take() {
        try {
            checkRemovedx();

            boolean peek = false;

            return blockReadOp(queryCallable(qryFactory.firstItemQuery(), new TerminalItemQueryLocalReducer<T>(true),
                peek), TAKE, peek);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear(int batchSize) throws GridRuntimeException {
        try {
            A.ensure(batchSize >= 0, "Batch size cannot be negative: " + batchSize);

            checkRemovedx();

            CU.outTx(clearCallable(batchSize), cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        checkRemoved();

        try {
            CU.outTx(clearCallable(0), cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        checkRemoved();

        GridCacheQueueHeader globalHdr;

        try {
            globalHdr = queueHdrView.get(key);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }

        assert globalHdr != null : "Failed to find queue header in cache: " + this;

        return globalHdr.empty();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        checkRemoved();

        GridCacheQueueHeader globalHdr;

        try {
            globalHdr = queueHdrView.get(key);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }

        assert globalHdr != null : "Failed to find queue header in cache: " + this;

        return globalHdr.size();
    }

    /** {@inheritDoc} */
    @Override public int remainingCapacity() {
        if (!bounded)
            return Integer.MAX_VALUE;

        int remaining = cap - size();

        return remaining > 0 ? remaining : 0;
    }

    /** {@inheritDoc} */
    @Override public int drainTo(Collection<? super T> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    /** {@inheritDoc} */
    @Override public int drainTo(Collection<? super T> c, int maxElements) {
        int max = Math.min(maxElements, size());

        for (int i = 0; i < max; i++) {
            T el = poll();

            if (el == null)
                return i;

            c.add(el);
        }

        return max;
    }

    /** {@inheritDoc} */
    @Override public int capacity() throws GridException {
        checkRemovedx();

        return cap;
    }

    /** {@inheritDoc} */
    @Override public boolean bounded() throws GridException {
        checkRemovedx();

        return bounded;
    }

    /** {@inheritDoc} */
    @Override public boolean collocated() throws GridException {
        checkRemovedx();

        return collocated;
    }

    /** {@inheritDoc} */
    @Override public boolean onRemoved() {
        synchronized (mux) {
            if (!rmvd)
                rmvd = true;

            // Free all blocked resources.
            writeSem.drainPermits();
            writeSem.release(Integer.MAX_VALUE);

            readSem.drainPermits();
            readSem.release(Integer.MAX_VALUE);
        }

        if (log.isDebugEnabled())
            log.debug("Queue has been removed: " + this);

        return rmvd;
    }

    /** {@inheritDoc} */
    @Override public void onInvalid(@Nullable Exception err) {
        synchronized (mux) {
            if (rmvd)
                return;

            this.err = err;

            // Free all blocked resources.
            writeSem.drainPermits();
            writeSem.release(Integer.MAX_VALUE);

            readSem.drainPermits();
            readSem.release(Integer.MAX_VALUE);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheInternalKey key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public void onHeaderChanged(GridCacheQueueHeader globalHdr) {
        // If queue is removed don't do anything.
        synchronized (mux) {
            if (rmvd)
                return;

            // Release read semaphore.
            if (!globalHdr.empty()) {
                readSem.drainPermits();
                readSem.release(globalHdr.size());
            }

            // Release write semaphore.
            if (bounded) {
                writeSem.drainPermits();

                if (!globalHdr.full())
                    writeSem.release(globalHdr.capacity() - globalHdr.size());
            }
        }

        if (log.isDebugEnabled())
            log.debug("Queue header has changed [hdr=" + globalHdr + ", queue=" + this + ']');
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return rmvd;
    }

    /**
     * Check removed status.
     *
     * @throws GridException If removed.
     */
    private void checkRemovedx() throws GridException {
        if (rmvd)
            throw new GridCacheDataStructureRemovedException("Queue has been removed from cache: " + this);

        if (err != null)
            throw new GridCacheDataStructureInvalidException("Queue is in invalid state " +
                "(discard this queue instance and get another from cache): " + this, err);
    }

    /**
     * Check removed status and throws GridRuntimeException if queue removed. Method is used for Ex methods.
     */
    private void checkRemoved() {
        if (rmvd)
            throw new GridCacheDataStructureRemovedRuntimeException("Queue has been removed from cache: " + this);


        if (err != null)
            throw new GridCacheDataStructureInvalidRuntimeException("Queue is in invalid state " +
                "(discard this queue instance and get another from cache): " + this, err);
    }

    /**
     * Method implements universal method for add object to queue.
     *
     * @param items Items.
     * @return Callable.
     */
    private Callable<Boolean> addCallable(final Collection<T> items) {
        return new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                if (items.contains(null))
                    throw new GridException("Queue item can't be null [queue=" + GridCacheQueueImpl.this + ", items=" +
                        items + ']');

                checkRemovedx();

                try (GridCacheTx tx = CU.txStartInternal(cctx, queueHdrView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheQueueHeader globalHdr = queueHdrView.get(key);

                    checkRemovedx();

                    assert globalHdr != null : "Failed to find queue header in cache [nodeId=" + cctx.localNodeId() +
                        ", cache=" + cctx.cache().name() + ", queue=" + GridCacheQueueImpl.this + ']';

                    if (globalHdr.full() || (bounded && globalHdr.size() + items.size() > cap)) {
                        tx.setRollbackOnly();

                        // Block all write attempts.
                        synchronized (mux) {
                            writeSem.drainPermits();
                        }

                        if (log.isDebugEnabled())
                            log.debug("Queue is full [globalHdr=" + globalHdr + ", queue=" + GridCacheQueueImpl.this +
                                ']');

                        return false;
                    }

                    Map<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> data = new HashMap<>();

                    // Prepare data.
                    for (T item : items) {
                        // Increment queue size and sequence.
                        globalHdr.incrementSize();

                        long seq = globalHdr.incrementSequence();

                        //Make queue item key.
                        GridCacheQueueItemKey itemKey = new GridCacheQueueItemKeyImpl(seq, qid, collocated);

                        // Make new queue item.
                        GridCacheQueueItemImpl<T> val = new GridCacheQueueItemImpl<>(qid, seq, item);

                        val.enqueueTime(U.currentTimeMillis());

                        data.put(itemKey, val);
                    }

                    // Put data to cache.
                    if (!data.isEmpty()) {
                        itemView.putAll(data);

                        // Update queue header.
                        queueHdrView.putx(key, globalHdr);
                    }

                    if (log.isDebugEnabled())
                        log.debug("Items will be added to queue [items=" + items + ", hdr=" + globalHdr + ", queue=" +
                            GridCacheQueueImpl.this + ']');

                    tx.commit();

                    return true;
                }
            }
        };
    }

    /**
     * Method implements universal method for getting object from queue.
     *
     * @param qry Query.
     * @param rdc Local reducer.
     * @param peek {@code true} don't release received queue item, {@code false} release received queue item.
     * @return Callable.
     */
    private Callable<T> queryCallable(final GridCacheQuery<Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>> qry, final GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
        Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> rdc, final boolean peek) {
        return new Callable<T>() {
            @Nullable @Override public T call() throws Exception {
                checkRemovedx();

                try (GridCacheTx tx = CU.txStartInternal(cctx, cctx.cache(), PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheQueueHeader globalHdr = queueHdrView.get(key);

                    checkRemovedx();

                    assert globalHdr != null : "Failed to find queue header in cache: " + GridCacheQueueImpl.this;

                    if (globalHdr.empty()) {
                        tx.setRollbackOnly();

                        // Block all readers.
                        synchronized (mux) {
                            readSem.drainPermits();
                        }

                        return null;
                    }

                    Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> entry;

                    if (collocated) {
                        // For case if primary node was changed during request.
                        while (true) {
                            GridNode node = CU.primaryNode(cctx, key);

                            qry.projection(cctx.grid().forNode(node));

                            entry = F.reduce(qry.execute(new OneRecordReducer<T>(), qid).get(), rdc);

                            if (log.isDebugEnabled())
                                log.debug("Entry has been found [node=" + node + ", entry=" + entry + ", queue=" +
                                    GridCacheQueueImpl.this + ']');

                            // Entry wasn't found perhaps grid topology was changed.
                            GridNode node2;

                            if (entry == null) node2 = CU.primaryNode(cctx, key);
                            else break;

                            // Topology wasn't changed.
                            if (node.equals(node2)) break;

                            if (log.isDebugEnabled())
                                log.debug("Entry wasn't found for queue: " + GridCacheQueueImpl.this);
                        }
                    }
                    else {
                        // In non-collocated mode query can return already removed entry. We should check this issue.
                        while (true) {
                            entry = F.reduce(qry.execute(new OneRecordReducer<T>(), qid).get(), rdc);

                            // We don't have eny items in queue.
                            if (entry == null) break;

                            GridCacheQueueItem<T> val = itemView.get(entry.getKey());

                            if (val == null) {
                                if (log.isDebugEnabled())
                                    log.debug("Already removed entry have been found [entry=" + entry + ", queue=" +
                                        GridCacheQueueImpl.this + ']');

                                // Re-remove the key as it was removed before.
                                itemView.remove(entry.getKey());
                            }
                            else {
                                if (log.isDebugEnabled())
                                    log.debug("Queue entry have been found [entry=" + entry + ", queue=" +
                                        GridCacheQueueImpl.this + ']');

                                break;
                            }
                        }
                    }

                    if (entry == null) {
                        tx.setRollbackOnly();

                        if (log.isDebugEnabled()) log.debug("Failed to find queue item in: " + GridCacheQueueImpl.this);

                        return null;
                    }

                    GridCacheQueueItem<T> val = entry.getValue();

                    assert val != null : "Failed to get entry value: " + entry;
                    assert val.userObject() != null : "Failed to get user object from value: " + entry;

                    if (!peek) {
                        // Check queue size.
                        assert globalHdr.size() > 0 : "Queue is empty but item has been found [item=" + val +
                            ", header=" + globalHdr + ", queue=" + GridCacheQueueImpl.this + ']';

                        globalHdr.decrementSize();

                        // Refresh queue header in cache.
                        queueHdrView.putx(key, globalHdr);

                        // Remove item from cache.
                        boolean wasRmvd = itemView.removex(entry.getKey());

                        assert wasRmvd : "Already deleted entry: " + entry;
                    }

                    tx.commit();

                    if (log.isDebugEnabled())
                        log.debug("Retrieved queue item [item=" + val + ", queue=" + GridCacheQueueImpl.this + ']');

                    return val.userObject();
                }
            }
        };
    }

    /**
     * Method implements universal method for clear queue.
     *
     * @param batchSize Batch size.
     * @return Callable for queue clearing .
     */
    private Callable<Boolean> clearCallable(final int batchSize) {
        return new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                checkRemovedx();

                // Result of operations.
                GridBiTuple<Integer, GridException> qryRes = new T2<>(0, null);

                int queueOldSize;

                try (GridCacheTx tx = CU.txStartInternal(cctx, cctx.cache(), PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheQueueHeader globalHdr = queueHdrView.get(key);

                    checkRemovedx();

                    assert globalHdr != null : "Failed to find queue header in cache: " + GridCacheQueueImpl.this;

                    GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> qry =
                        qryFactory.removeAllKeysQuery();

                    if (collocated) {
                        // For case if primary node was changed during request.
                        while (true) {
                            GridNode node = CU.primaryNode(cctx, key);

                            qry.projection(cctx.grid().forNode(node));

                            GridBiTuple<Integer, GridException> tup =
                                F.reduce(qry.execute(qryFactory.removeAllKeysReducer(batchSize), qid).get(),
                                    new RemoveAllKeysQueryLocalReducer());

                            // Check topology changes.
                            GridNode node2 = CU.primaryNode(cctx, key);

                            // If topology wasn't changed then break loop.
                            if (node.equals(node2)) {
                                qryRes = tup;

                                break;
                            }
                            else
                                qryRes.set(qryRes.get1() + tup.get1(), tup.get2() != null ? tup.get2() : qryRes.get2());

                            if (log.isDebugEnabled())
                                log.debug("Node topology was changed, request will be repeated for queue: " +
                                    GridCacheQueueImpl.this);
                        }
                    }
                    else
                        qryRes = F.reduce(qry.execute(qryFactory.removeAllKeysReducer(batchSize), qid).get(),
                            new RemoveAllKeysQueryLocalReducer());

                    assert qryRes != null;

                    // Queue old size.
                    queueOldSize = globalHdr.size();

                    // Update queue header in any case because some queue items can be already removed.
                    globalHdr.size(globalHdr.size() - qryRes.get1());

                    queueHdrView.putx(key, globalHdr);

                    tx.commit();

                    if (log.isDebugEnabled())
                        log.debug("Items were removed [itemsNumber=" + qryRes.get1() + ", queueHeader=" + globalHdr +
                            ", queue=" + GridCacheQueueImpl.this + ']');

                    if (queueOldSize != qryRes.get1() && log.isDebugEnabled())
                        log.debug("Queue size mismatch [itemsNumber=" + qryRes.get1() +
                            ", headerOldSize=" + queueOldSize + ", newHeader=" + globalHdr + ", queue=" +
                            GridCacheQueueImpl.this + ']');
                }

                // Throw remote exception if it's happened.
                if (qryRes.get2() != null)
                    throw qryRes.get2();

                return queueOldSize == qryRes.get1();
            }
        };
    }

    /**
     * Method implements universal method for remove queue items.
     *
     * @param items Items.
     * @param retain If {@code true} will be removed all queue items instead of {@code items}, {@code false} only all
     *      {@code items} will be removed from queue.
     * @return Callable for removing queue item.
     */
    private Callable<Boolean> removeItemsCallable(final Collection<T> items, final boolean retain) {
        return new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                checkRemovedx();

                // Result of operations.
                GridBiTuple<Integer, GridException> qryRes = new T2<>(0, null);

                // Prepare id's for query.
                Collection<Integer> hashes = new ArrayList<>(items.size());

                for (T item : items)
                    hashes.add(item.hashCode());

                try (GridCacheTx tx = CU.txStartInternal(cctx, cctx.cache(), PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheQueueHeader globalHdr = queueHdrView.get(key);

                    checkRemovedx();

                    assert globalHdr != null : "Failed to find queue header in cache: " + GridCacheQueueImpl.this;

                    if (globalHdr.empty()) {
                        tx.setRollbackOnly();

                        return false;
                    }

                    GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> qry =
                        qryFactory.itemsKeysQuery();

                    if (collocated) {
                        // For case if primary node was changed during request.
                        while (true) {
                            GridNode node = CU.primaryNode(cctx, key);

                            qry.projection(cctx.grid().forNode(node));

                            GridBiTuple<Integer, GridException> tup = F.reduce(qry.execute(qryFactory
                                .itemsKeysReducer(items, retain, items.size() == 1), qid, hashes.toArray()).
                                get(), new RemoveItemsQueryLocalReducer());

                            // Check topology changes.
                            GridNode node2 = CU.primaryNode(cctx, key);

                            // If topology wasn't changed then break loop.
                            if (node.equals(node2)) {
                                qryRes = tup;

                                break;
                            }
                            else
                                qryRes.set(qryRes.get1() + tup.get1(), tup.get2() != null ? tup.get2() : qryRes.get2());

                            if (log.isDebugEnabled())
                                log.debug("Node topology was changed, request will be repeated for queue: " +
                                    GridCacheQueueImpl.this);
                        }
                    }
                    else
                        qryRes = F.reduce(qry.execute(qryFactory.itemsKeysReducer(items, retain, items.size() == 1),
                            qid, hashes.toArray()).
                            get(), new RemoveItemsQueryLocalReducer());

                    assert qryRes != null;

                    if (qryRes.get1() > 0) {
                        // Update queue header in any case because some queue items can be already removed.
                        globalHdr.size(globalHdr.size() - qryRes.get1());

                        queueHdrView.putx(key, globalHdr);

                        tx.commit();

                        if (log.isDebugEnabled())
                            log.debug("Items were removed [itemsNumber=" + qryRes.get1() + ", queueHeader=" +
                                globalHdr + ", queue=" + GridCacheQueueImpl.this + ']');
                    }
                }

                // Throw remote exception if it's happened.
                if (qryRes.get2() != null)
                    throw qryRes.get2();

                // At least one object was removed or retained.
                return qryRes.get1() > 0;
            }
        };
    }

    /**
     * Method implements universal blocking read operation without exactly timeout.
     *
     * @param cmd Callable for execution.
     * @param op Operation name.
     * @param peek {@code true} don't release received queue item, {@code false} release received queue item.
     * @return Queue item.
     * @throws GridException If failed.
     */
    private T blockReadOp(Callable<T> cmd, GridCacheQueueOperation op, boolean peek) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Read operation will be blocked [op=" + op + ", queue=" + this + ']');

        int attempts = 0;

        T retVal;

        // Operation will be repeated until success.
        while (true) {
            checkRemovedx();

            attempts++;

            try {
                readSem.acquire();

                // We should make redSem the same if we don't remove item from queue after reading.
                if (peek)
                    synchronized (mux) {
                        checkRemovedx();

                        readSem.release();
                    }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridInterruptedException("Operation has been interrupted [op=" + op +
                    ", queue=" + this + ']', e);
            }

            retVal = CU.outTx(cmd, cctx);

            // Break loop in case operation executed successfully.
            if (retVal != null)
                break;

            if (attempts % blockAttemptWarnThreshold == 0) {
                if (log.isDebugEnabled())
                    log.debug("Exceeded warning threshold for execution attempts [op=" + op +
                        ", attempts=" + attempts + ", queue=" + this + ']');
            }
        }

        if (log.isDebugEnabled())
            log.debug("Operation unblocked [op=" + op + ", retVal=" + retVal + ", queue=" + this + ']');

        return retVal;
    }

    /**
     * Method implements universal blocking read operation with exactly timeout.
     *
     * @param cmd Callable for execution.
     * @param op Operation name.
     * @param timeout Timeout.
     * @param unit a TimeUnit determining how to interpret the timeout parameter.
     * @param peek {@code true} don't release received queue item, {@code false} release received queue item.
     * @return Queue item.
     * @throws GridException If failed.
     */
    @Nullable private T blockReadOp(Callable<T> cmd, GridCacheQueueOperation op, long timeout, TimeUnit unit,
        boolean peek)
        throws GridException {
        long end = U.currentTimeMillis() + MILLISECONDS.convert(timeout, unit);

        if (log.isDebugEnabled())
            log.debug("Read operation will be blocked on timeout [op=" + op + ", timeout=" + end + ", queue=" + this +
                ']');

        T retVal = null;

        // Operation will be repeated until success or timeout will be expired.
        int attempts = 0;

        while (end - U.currentTimeMillis() > 0) {
            checkRemovedx();

            attempts++;

            try {
                if (readSem.tryAcquire(end - U.currentTimeMillis(), MILLISECONDS)) {
                    // We should make redSem the same if we don't remove item from queue after reading.
                    if (peek)
                        synchronized (mux) {
                            checkRemovedx();

                            readSem.release();
                        }

                    retVal = CU.outTx(cmd, cctx);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridInterruptedException("Operation has been interrupted [op=" + op +
                    ", queue=" + this + ']', e);
            }

            // Break loop in case operation executed successfully.
            if (retVal != null)
                break;

            if (attempts % blockAttemptWarnThreshold == 0) {
                if (log.isDebugEnabled())
                    log.debug("Exceeded warning threshold for execution attempts [op=" + op +
                        ", attempts=" + attempts + ", queue=" + this + ']');
            }
        }

        if (log.isDebugEnabled())
            log.debug("Operation unblocked on timeout [op=" + op + ", retVal=" + retVal + ", queue=" + this + ']');

        return retVal;
    }

    /**
     * Method implements universal blocking write operation.
     *
     * @param items Item for putting to the queue.
     * @param op Operation name.
     * @throws GridException If failed.
     */
    private void blockWriteOp(Collection<T> items, GridCacheQueueOperation op) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Operation will be blocked [op=" + op + ", queue=" + this + ']');

        // Operation will be repeated until success.
        int attempts = 0;

        while (true) {
            attempts++;

            try {
                writeSem.acquire(items.size());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridInterruptedException("Operation has been interrupted [op=" + op +
                    ", queue=" + this + ']', e);
            }

            checkRemovedx();

            boolean retVal = CU.outTx(addCallable(items), cctx);

            // Break loop in case operation executed successfully.
            if (retVal) {
                if (log.isDebugEnabled())
                    log.debug("Operation unblocked [op=" + op + ", retVal=" + retVal + ", queue=" + this + ']');

                return;
            }

            if (attempts % blockAttemptWarnThreshold == 0) {
                if (log.isDebugEnabled())
                    log.debug("Exceeded warning threshold for execution attempts [op=" + op +
                        ", attempts=" + attempts + ", queue=" + this + ']');
            }
        }
    }

    /**
     * Method implements universal blocking write operation with exactly timeout.
     *
     * @param items Items for putting to the queue.
     * @param op Operation name.
     * @param timeout Timeout.
     * @param unit a TimeUnit determining how to interpret the timeout parameter.
     * @return {@code "true"} if item was added successfully, {@code "false"} if timeout was expired.
     * @throws GridException If failed.
     */
    private boolean blockWriteOp(Collection<T> items, GridCacheQueueOperation op, long timeout, TimeUnit unit)
        throws GridException {
        long end = U.currentTimeMillis() + MILLISECONDS.convert(timeout, unit);

        if (log.isDebugEnabled())
            log.debug("Write operation will be blocked on timeout [op=" + op + ", timeout=" + end +
                ", queue=" + this + ']');

        int attempts = 0;

        boolean retVal = false;

        // Operation will be repeated until success.
        while (end - U.currentTimeMillis() > 0) {
            checkRemovedx();

            attempts++;

            try {
                if (writeSem.tryAcquire(items.size(), end - U.currentTimeMillis(), MILLISECONDS)) {
                    checkRemovedx();

                    retVal = CU.outTx(addCallable(items), cctx);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridInterruptedException("Operation has been interrupted [op=" + op +
                    ", queue=" + this + ']', e);
            }

            // Break loop in case operation executed successfully.
            if (retVal)
                break;

            if (attempts % blockAttemptWarnThreshold == 0) {
                if (log.isDebugEnabled())
                    log.debug("Exceeded warning threshold for execution attempts [op=" + op +
                        ", attempts=" + attempts + ", queue=" + this + ']');
            }
        }

        if (log.isDebugEnabled())
            log.debug("Operation unblocked on timeout [op=" + op + ", retVal=" + retVal + ", queue=" + this + ']');

        return retVal;
    }

    /**
     * Remove all queue items and queue header.
     *
     * @return Callable for queue clearing .
     * @throws GridException If queue already removed or operation failed.
     */
    @Override public boolean removeQueue(int batchSize) throws GridException {
        checkRemovedx();

        return CU.outTx(removeQueueCallable(batchSize), cctx);
    }

    /**
     * Remove all queue items and queue header.
     *
     * @param batchSize Batch size.
     * @return Callable for queue clearing .
     */
    private Callable<Boolean> removeQueueCallable(final int batchSize) {
        return new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("Try to remove queue: " + GridCacheQueueImpl.this);

                checkRemovedx();

                // Result of operations.
                boolean res = false;

                // Query execution result.
                GridBiTuple<Integer, GridException> qryRes = new T2<>(0, null);

                try (GridCacheTx tx = CU.txStartInternal(cctx, cctx.cache(), PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheQueueHeader globalHdr = queueHdrView.get(key);

                    checkRemovedx();

                    assert globalHdr != null : "Failed to find queue header in cache: " + GridCacheQueueImpl.this;

                    GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> qry =
                        qryFactory.removeAllKeysQuery();

                    if (collocated) {
                        // For case if primary node was changed during request.
                        while (true) {
                            GridNode node = CU.primaryNode(cctx, key);

                            qry = qry.projection(cctx.grid().forNode(node));

                            GridBiTuple<Integer, GridException> tup =
                                F.reduce(qry.execute(qryFactory.removeAllKeysReducer(batchSize), qid).get(),
                                    new RemoveAllKeysQueryLocalReducer());

                            // Check topology changes.
                            GridNode node2 = CU.primaryNode(cctx, key);

                            // If topology wasn't changed then break loop.
                            if (node.equals(node2)) {
                                qryRes = tup;

                                break;
                            }
                            else
                                qryRes.set(qryRes.get1() + tup.get1(), tup.get2() != null ? tup.get2() : qryRes.get2());

                            if (log.isDebugEnabled())
                                log.debug("Node topology was changed, request will be repeated for queue: " +
                                    GridCacheQueueImpl.this);
                        }
                    }
                    else
                        qryRes = F.reduce(qry.execute(qryFactory.removeAllKeysReducer(batchSize), qid).get(),
                            new RemoveAllKeysQueryLocalReducer());

                    assert qryRes != null;

                    // Queue old size.
                    int queueOldSize = globalHdr.size();

                    if (queueOldSize != qryRes.get1()) {
                        assert queueOldSize > qryRes.get1() : "Queue size mismatch [old=" + queueOldSize + ", rcvd=" +
                            qryRes.get1() + ", queue=" + this + ']';

                        // Update queue header in any case because some queue items can be already removed.
                        globalHdr.size(globalHdr.size() - qryRes.get1());

                        queueHdrView.putx(key, globalHdr);
                    }
                    else {
                        res = true;

                        queueHdrView.removex(key);

                        if (log.isDebugEnabled()) log.debug("Queue will be removed: " + GridCacheQueueImpl.this);
                    }

                    tx.commit();

                    if (queueOldSize != qryRes.get1() && log.isDebugEnabled())
                        log.debug("Queue size mismatch [itemsNumber=" + qryRes.get1() +
                            ", headerOldSize=" + queueOldSize + ", newHeader=" + globalHdr + ", queue=" +
                            GridCacheQueueImpl.this + ']');
                }

                // Throw remote exception if it's happened.
                if (qryRes.get2() != null)
                    throw qryRes.get2();

                return res;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        checkRemoved();

        try {
            GridCacheQueueHeader globalHdr = queueHdrView.get(key);

            assert globalHdr != null;

            return new GridCacheQueueIterator();
        }
        catch (GridException e) {
            throw new GridRuntimeException("Failed to create iterator in queue: " + this, e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object[] toArray() {
        checkRemoved();

        return super.toArray();
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"SuspiciousToArrayCall"})
    @Override public <T> T[] toArray(T[] a) {
        A.notNull(a, "a");

        checkRemoved();

        return super.toArray(a);
    }

    /**
     * Queue iterator.
     */
    private class GridCacheQueueIterator implements Iterator<T> {
        /** Default page size. */
        private static final int DFLT_PAGE_SIZE = 10;

        /** Query iterator. */
        private final Iterator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> iter;

        /** Current entry. */
        private Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> entry;

        /**
         * Default constructor.
         *
         * @throws GridException In case of error.
         */
        private GridCacheQueueIterator() throws GridException {
            GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> qry =
                qryFactory.itemsQuery();

            qry.pageSize(DFLT_PAGE_SIZE);

            GridProjection prj = cctx.grid().forNode(CU.primaryNode(cctx, key));

            if (collocated)
                qry.projection(prj);

            iter = qry.execute(qid).get().iterator();

            assert iter != null;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            checkRemoved();

            return iter.hasNext();
        }

        /** {@inheritDoc} */
        @Nullable @Override public T next() {
            checkRemoved();

            entry = iter.next();

            assert entry != null;
            assert entry.getValue() != null;

            return entry.getValue().userObject();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            checkRemoved();

            // If entry hasn't been set.
            if (entry == null)
                throw new IllegalStateException("Remove cannot be called twice without advancing iterator.");

            // Save locally current iterator element.
            final Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> locEntry = entry;

            try {
                // Try to get item from cache.
                if (itemView.get(locEntry.getKey()) == null)
                    return;

                boolean res = CU.outTx(new Callable<Boolean>() {
                        @Override public Boolean call() throws Exception {

                            try (GridCacheTx tx = CU.txStartInternal(cctx, cctx.cache(), PESSIMISTIC,
                                REPEATABLE_READ)) {
                                GridCacheQueueHeader globalHdr = queueHdrView.get(key);

                                checkRemovedx();

                                assert globalHdr != null : "Failed to find queue header in cache: " +
                                    GridCacheQueueImpl.this;

                                if (globalHdr.empty()) {
                                    tx.setRollbackOnly();

                                    return false;
                                }

                                // Remove item from cache.
                                if (itemView.removex(locEntry.getKey())) {
                                    if (log.isDebugEnabled())
                                        log.debug("Removing queue item [item=" + locEntry.getValue() + ", queue=" +
                                            this + ']');

                                    globalHdr.decrementSize();

                                    // Update queue header.
                                    queueHdrView.putx(key, globalHdr);
                                }
                                else {
                                    tx.setRollbackOnly();

                                    if (log.isDebugEnabled())
                                        log.debug("Queue item has been removed [item=" + locEntry.getValue() +
                                            ", queue=" + this + ']');

                                    return false;
                                }

                                tx.commit();

                                return true;
                            }
                        }
                    }, cctx);

                if (res)
                    entry = null;
            }
            catch (GridException e) {
                U.error(log, "Failed to remove item: " + entry, e);

                throw new GridRuntimeException("Failed to remove item: " + entry, e);
            }
        }

    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(cctx);
        out.writeUTF(qid);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        GridBiTuple<GridCacheContext, String> t = stash.get();

        t.set1((GridCacheContext)in.readObject());
        t.set2(in.readUTF());
    }

    /**
     * Reconstructs object on demarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of demarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            GridBiTuple<GridCacheContext, String> t = stash.get();

            return t.get1().dataStructures().queue(t.get2(), Integer.MAX_VALUE, true, false);
        }
        catch (GridException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueueImpl.class, this);
    }
}
