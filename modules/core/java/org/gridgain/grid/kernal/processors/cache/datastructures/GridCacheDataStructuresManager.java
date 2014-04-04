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
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.query.continuous.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;

/**
 * Manager of data structures.
 */
public final class GridCacheDataStructuresManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Initial capacity. */
    private static final int INITIAL_CAPACITY = 10;

    /** Number of queue items to process before delaying cleanup. */
    private static final long QUEUE_CLEANUP_THROTTLE_ITEMS = 10_000;

    /** Queue cleanup throttle delay. */
    private static final long QUEUE_CLEANUP_THROTTLE_DELAY = 5_000;

    /** Cache contains only {@code GridCacheInternal,GridCacheInternal}. */
    private GridCacheProjection<GridCacheInternal, GridCacheInternal> dsView;

    /** Internal storage of all dataStructures items (sequence, queue , atomic long etc.). */
    private final ConcurrentMap<GridCacheInternal, GridCacheRemovable> dsMap;

    /** Queues map. */
    private final ConcurrentMap<GridUuid, GridCacheQueueProxy> queuesMap;

    /** Query notifying about queue update. */
    private GridCacheContinuousQueryAdapter queueQry;

    /** Cache contains only {@code GridCacheAtomicValue}. */
    private GridCacheProjection<GridCacheInternalKey, GridCacheAtomicLongValue> atomicLongView;

    /** Cache contains only {@code GridCacheCountDownLatchValue}. */
    private GridCacheProjection<GridCacheInternalKey, GridCacheCountDownLatchValue> cntDownLatchView;

    /** Cache contains only {@code GridCacheAtomicReferenceValue}. */
    private GridCacheProjection<GridCacheInternalKey, GridCacheAtomicReferenceValue> atomicRefView;

    /** Cache contains only {@code GridCacheAtomicStampedValue}. */
    private GridCacheProjection<GridCacheInternalKey, GridCacheAtomicStampedValue> atomicStampedView;

    /** Cache contains only entry {@code GridCacheSequenceValue}.  */
    private GridCacheProjection<GridCacheInternalKey, GridCacheAtomicSequenceValue> seqView;

    /** Cache contains only entry {@code GridCacheQueueHeader}.  */
    private GridCacheProjection<GridCacheQueueHeaderKey, GridCacheQueueHeader> queueHdrView;

    /** Workers removing orphaned queue items. */
    private List<QueueCleanupWorker> queueCleanupWorkers;

    /** Init latch. */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Init flag. */
    private boolean initFlag;

    /**
     * Default constructor.
     */
    public GridCacheDataStructuresManager() {
        dsMap = new ConcurrentHashMap8<>(INITIAL_CAPACITY);
        queuesMap = new ConcurrentHashMap8<>(INITIAL_CAPACITY);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void onKernalStart0() throws GridException {
        try {
            dsView = cctx.cache().<GridCacheInternal, GridCacheInternal>projection
                (GridCacheInternal.class, GridCacheInternal.class).flagsOn(CLONE);

            if (transactionalWithNear()) {
                cntDownLatchView = cctx.cache().<GridCacheInternalKey, GridCacheCountDownLatchValue>projection
                    (GridCacheInternalKey.class, GridCacheCountDownLatchValue.class).flagsOn(CLONE);

                atomicLongView = cctx.cache().<GridCacheInternalKey, GridCacheAtomicLongValue>projection
                    (GridCacheInternalKey.class, GridCacheAtomicLongValue.class).flagsOn(CLONE);

                atomicRefView = cctx.cache().<GridCacheInternalKey, GridCacheAtomicReferenceValue>projection
                    (GridCacheInternalKey.class, GridCacheAtomicReferenceValue.class).flagsOn(CLONE);

                atomicStampedView = cctx.cache().<GridCacheInternalKey, GridCacheAtomicStampedValue>projection
                    (GridCacheInternalKey.class, GridCacheAtomicStampedValue.class).flagsOn(CLONE);

                seqView = cctx.cache().<GridCacheInternalKey, GridCacheAtomicSequenceValue>projection
                    (GridCacheInternalKey.class, GridCacheAtomicSequenceValue.class).flagsOn(CLONE);
            }

            if (supportsQueue()) {
                assert cctx.config().getCacheQueueCleanupFrequency() > 0;
                assert cctx.config().getCacheQueueCleanupThreadsCount() > 0;

                queueHdrView = cctx.cache().<GridCacheQueueHeaderKey, GridCacheQueueHeader>projection
                    (GridCacheQueueHeaderKey.class, GridCacheQueueHeader.class).flagsOn(CLONE);

                int cleanupWorkersCnt = cctx.config().getCacheQueueCleanupThreadsCount();

                queueCleanupWorkers = new ArrayList<>(cleanupWorkersCnt);

                for (int i = 0; i < cleanupWorkersCnt; i++) {
                    QueueCleanupWorker worker = new QueueCleanupWorker(cleanupWorkersCnt, i);

                    queueCleanupWorkers.add(worker);

                    new GridThread(worker).start();
                }

                queueQry = (GridCacheContinuousQueryAdapter)cctx.cache().queries().createContinuousQuery();

                queueQry.filter(new QueueHeaderPredicate());

                queueQry.callback(new GridBiPredicate<UUID, Collection<Map.Entry>>() {
                    @Override public boolean apply(UUID id, Collection<Map.Entry> entries) {
                        for (Map.Entry e : entries) {
                            GridCacheQueueHeaderKey key = (GridCacheQueueHeaderKey)e.getKey();
                            GridCacheQueueHeader hdr = (GridCacheQueueHeader)e.getValue();

                            if (hdr == null) {
                                for (QueueCleanupWorker worker : queueCleanupWorkers)
                                    worker.wakeUp();
                            }

                            for (final GridCacheQueueProxy queue : queuesMap.values()) {
                                if (queue.name().equals(key.queueName())) {
                                    if (hdr == null) {
                                        /*
                                         * Potentially there can be queues with the same names, need to check that
                                         * queue was really removed.
                                         */
                                        cctx.closures().callLocalSafe(new Callable<Void>() {
                                            @Override public Void call() throws Exception {
                                                try {
                                                    queue.size();
                                                }
                                                catch (GridCacheDataStructureRemovedRuntimeException ignore) {
                                                    queuesMap.remove(queue.delegate().id());
                                                }

                                                return null;
                                            }
                                        }, false);
                                    }
                                    else
                                        queue.delegate().onHeaderChanged(hdr);
                                }
                            }
                        }

                        return true;
                    }
                });

                queueQry.execute(cctx.isLocal() || cctx.isReplicated() ? cctx.grid().forLocal() : null, true);
            }

            initFlag = true;
        }
        finally {
            initLatch.countDown();
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        super.onKernalStop0(cancel);

        if (queueQry != null) {
            try {
                queueQry.close();
            }
            catch (GridException e) {
                U.warn(log, "Failed to cancel queue header query.", e);
            }
        }

        if (queueCleanupWorkers != null) {
            for (QueueCleanupWorker worker : queueCleanupWorkers)
                U.cancel(worker);

            for (QueueCleanupWorker worker : queueCleanupWorkers)
                U.join(worker, log);
        }

        for (GridCacheQueueProxy q : queuesMap.values())
            q.delegate().onKernalStop();
    }

    /**
     * Gets a sequence from cache or creates one if it's not cached.
     *
     * @param name Sequence name.
     * @param initVal Initial value for sequence. If sequence already cached, {@code initVal} will be ignored.
     * @param create  If {@code true} sequence will be created in case it is not in cache.
     * @return Sequence.
     * @throws GridException If loading failed.
     */
    public final GridCacheAtomicSequence sequence(final String name, final long initVal,
        final boolean create) throws GridException {
        waitInitialization();

        checkTransactionalWithNear();

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheAtomicSequence val = cast(dsMap.get(key), GridCacheAtomicSequence.class);

            if (val != null)
                return val;

            return CU.outTx(new Callable<GridCacheAtomicSequence>() {
                @Override public GridCacheAtomicSequence call() throws Exception {
                    try (GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                        GridCacheAtomicSequenceValue seqVal = cast(dsView.get(key),
                            GridCacheAtomicSequenceValue.class);

                        // Check that sequence hasn't been created in other thread yet.
                        GridCacheAtomicSequenceEx seq = cast(dsMap.get(key), GridCacheAtomicSequenceEx.class);

                        if (seq != null) {
                            assert seqVal != null;

                            return seq;
                        }

                        if (seqVal == null && !create)
                            return null;

                            /* We should use offset because we already reserved left side of range.*/
                        long off = cctx.config().getAtomicSequenceReserveSize() > 1 ?
                            cctx.config().getAtomicSequenceReserveSize() - 1 : 1;

                        long upBound;
                        long locCntr;

                        if (seqVal == null) {
                            locCntr = initVal;

                            upBound = locCntr + off;

                            // Global counter must be more than reserved region.
                            seqVal = new GridCacheAtomicSequenceValue(upBound + 1);
                        }
                        else {
                            locCntr = seqVal.get();

                            upBound = locCntr + off;

                            // Global counter must be more than reserved region.
                            seqVal.set(upBound + 1);
                        }

                        // Update global counter.
                        dsView.putx(key, seqVal);

                        // Only one thread can be in the transaction scope and create sequence.
                        seq = new GridCacheAtomicSequenceImpl(name, key, seqView, cctx,
                            locCntr, upBound);

                        dsMap.put(key, seq);

                        tx.commit();

                        return seq;
                    }
                    catch (Error | Exception e) {
                        dsMap.remove(key);

                        U.error(log, "Failed to make atomic sequence: " + name, e);

                        throw e;
                    }
                }
            }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get sequence by name: " + name, e);
        }
    }

    /**
     * Removes sequence from cache.
     *
     * @param name Sequence name.
     * @return Method returns {@code true} if sequence has been removed and {@code false} if it's not cached.
     * @throws GridException If removing failed.
     */
    public final boolean removeSequence(String name) throws GridException {
        waitInitialization();

        checkTransactionalWithNear();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicSequenceValue.class);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove sequence by name: " + name, e);
        }
    }

    /**
     * Gets an atomic long from cache or creates one if it's not cached.
     *
     * @param name Name of atomic long.
     * @param initVal Initial value for atomic long. If atomic long already cached, {@code initVal}
     *        will be ignored.
     * @param create If {@code true} atomic long will be created in case it is not in cache.
     * @return Atomic long.
     * @throws GridException If loading failed.
     */
    public final GridCacheAtomicLong atomicLong(final String name, final long initVal,
        final boolean create) throws GridException {
        waitInitialization();

        checkTransactionalWithNear();

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheAtomicLong atomicLong = cast(dsMap.get(key), GridCacheAtomicLong.class);

            if (atomicLong != null)
                return atomicLong;

            return CU.outTx(new Callable<GridCacheAtomicLong>() {
                @Override public GridCacheAtomicLong call() throws Exception {
                    try (GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                        GridCacheAtomicLongValue val = cast(dsView.get(key),
                            GridCacheAtomicLongValue.class);

                        // Check that atomic long hasn't been created in other thread yet.
                        GridCacheAtomicLongEx a = cast(dsMap.get(key), GridCacheAtomicLongEx.class);

                        if (a != null) {
                            assert val != null;

                            return a;
                        }

                        if (val == null && !create)
                            return null;

                        if (val == null) {
                            val = new GridCacheAtomicLongValue(initVal);

                            dsView.putx(key, val);
                        }

                        a = new GridCacheAtomicLongImpl(name, key, atomicLongView, cctx);

                        dsMap.put(key, a);

                        tx.commit();

                        return a;
                    }
                    catch (Error | Exception e) {
                        dsMap.remove(key);

                        U.error(log, "Failed to make atomic long: " + name, e);

                        throw e;
                    }
                }
            }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get atomic long by name: " + name, e);
        }
    }

    /**
     * Removes atomic long from cache.
     *
     * @param name Atomic long name.
     * @return Method returns {@code true} if atomic long has been removed and {@code false} if it's not cached.
     * @throws GridException If removing failed.
     */
    public final boolean removeAtomicLong(String name) throws GridException {
        waitInitialization();

        checkTransactionalWithNear();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicLongValue.class);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove atomic long by name: " + name, e);
        }
    }

    /**
     * Gets an atomic reference from cache or creates one if it's not cached.
     *
     * @param name Name of atomic reference.
     * @param initVal Initial value for atomic reference. If atomic reference already cached, {@code initVal}
     *        will be ignored.
     * @param create If {@code true} atomic reference will be created in case it is not in cache.
     * @return Atomic reference.
     * @throws GridException If loading failed.
     */
    @SuppressWarnings("unchecked")
    public final <T> GridCacheAtomicReference<T> atomicReference(final String name, final T initVal,
        final boolean create) throws GridException {
        waitInitialization();

        checkTransactionalWithNear();

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheAtomicReference atomicRef = cast(dsMap.get(key), GridCacheAtomicReference.class);

            if (atomicRef != null)
                return atomicRef;

            return CU.outTx(new Callable<GridCacheAtomicReference<T>>() {
                @Override public GridCacheAtomicReference<T> call() throws Exception {
                    try (GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                        GridCacheAtomicReferenceValue val = cast(dsView.get(key),
                            GridCacheAtomicReferenceValue.class);

                        // Check that atomic reference hasn't been created in other thread yet.
                        GridCacheAtomicReferenceEx ref = cast(dsMap.get(key),
                            GridCacheAtomicReferenceEx.class);

                        if (ref != null) {
                            assert val != null;

                            return ref;
                        }

                        if (val == null && !create)
                            return null;

                        if (val == null) {
                            val = new GridCacheAtomicReferenceValue(initVal);

                            dsView.putx(key, val);
                        }

                        ref = new GridCacheAtomicReferenceImpl(name, key, atomicRefView, cctx);

                        dsMap.put(key, ref);

                        tx.commit();

                        return ref;
                    }
                    catch (Error | Exception e) {
                        dsMap.remove(key);

                        U.error(log, "Failed to make atomic reference: " + name, e);

                        throw e;
                    }
                }
            }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get atomic reference by name: " + name, e);
        }
    }

    /**
     * Removes atomic reference from cache.
     *
     * @param name Atomic reference name.
     * @return Method returns {@code true} if atomic reference has been removed and {@code false} if it's not cached.
     * @throws GridException If removing failed.
     */
    public final boolean removeAtomicReference(String name) throws GridException {
        waitInitialization();

        checkTransactionalWithNear();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicReferenceValue.class);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove atomic reference by name: " + name, e);
        }
    }

    /**
     * Gets an atomic stamped from cache or creates one if it's not cached.
     *
     * @param name Name of atomic stamped.
     * @param initVal Initial value for atomic stamped. If atomic stamped already cached, {@code initVal}
     *        will be ignored.
     * @param initStamp Initial stamp for atomic stamped. If atomic stamped already cached, {@code initStamp}
     *        will be ignored.
     * @param create If {@code true} atomic stamped will be created in case it is not in cache.
     * @return Atomic stamped.
     * @throws GridException If loading failed.
     */
    @SuppressWarnings("unchecked")
    public final <T, S> GridCacheAtomicStamped<T, S> atomicStamped(final String name, final T initVal,
        final S initStamp, final boolean create) throws GridException {
        waitInitialization();

        checkTransactionalWithNear();

        final GridCacheInternalKeyImpl key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheAtomicStamped atomicStamped = cast(dsMap.get(key), GridCacheAtomicStamped.class);

            if (atomicStamped != null)
                return atomicStamped;

            return CU.outTx(new Callable<GridCacheAtomicStamped<T, S>>() {
                @Override public GridCacheAtomicStamped<T, S> call() throws Exception {
                    try (GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                        GridCacheAtomicStampedValue val = cast(dsView.get(key),
                            GridCacheAtomicStampedValue.class);

                        // Check that atomic stamped hasn't been created in other thread yet.
                        GridCacheAtomicStampedEx stmp = cast(dsMap.get(key),
                            GridCacheAtomicStampedEx.class);

                        if (stmp != null) {
                            assert val != null;

                            return stmp;
                        }

                        if (val == null && !create)
                            return null;

                        if (val == null) {
                            val = new GridCacheAtomicStampedValue(initVal, initStamp);

                            dsView.putx(key, val);
                        }

                        stmp = new GridCacheAtomicStampedImpl(name, key, atomicStampedView, cctx);

                        dsMap.put(key, stmp);

                        tx.commit();

                        return stmp;
                    }
                    catch (Error | Exception e) {
                        dsMap.remove(key);

                        U.error(log, "Failed to make atomic stamped: " + name, e);

                        throw e;
                    }
                }
            }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get atomic stamped by name: " + name, e);
        }
    }

    /**
     * Removes atomic stamped from cache.
     *
     * @param name Atomic stamped name.
     * @return Method returns {@code true} if atomic stamped has been removed and {@code false} if it's not cached.
     * @throws GridException If removing failed.
     */
    public final boolean removeAtomicStamped(String name) throws GridException {
        waitInitialization();

        checkTransactionalWithNear();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicStampedValue.class);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove atomic stamped by name: " + name, e);
        }
    }

    /**
     * Gets a queue from cache or creates one if it's not cached.
     *
     * @param name Name of queue.
     * @param cap Max size of queue.
     * @param colloc Collocation flag.
     * @param create If {@code true} queue will be created in case it is not in cache.
     * @return Instance of queue.
     * @throws GridException If failed.
     */
    public final <T> GridCacheQueue<T> queue(final String name, final int cap, boolean colloc,
        final boolean create) throws GridException {
        waitInitialization();

        checkSupportsQueue();

        // Non collocated mode enabled only for PARTITIONED cache.
        final boolean collocMode = cctx.cache().configuration().getCacheMode() != PARTITIONED || colloc;

        if (cctx.atomic())
            return queue0(name, cap, collocMode, create);

        return CU.outTx(new Callable<GridCacheQueue<T>>() {
            @Override public GridCacheQueue<T> call() throws Exception {
                return queue0(name, cap, collocMode, create);
            }
        }, cctx);
    }

    /**
     * Gets or creates queue.
     *
     * @param name Queue name.
     * @param cap Capacity.
     * @param colloc Collocation flag.
     * @param create If {@code true} queue will be created in case it is not in cache.
     * @return Queue.
     * @throws GridException If failed.
     */
    @SuppressWarnings({"unchecked", "NonPrivateFieldAccessedInSynchronizedContext"})
    private <T> GridCacheQueue<T> queue0(final String name, final int cap, boolean colloc, final boolean create)
        throws GridException {
        GridCacheQueueHeaderKey key = new GridCacheQueueHeaderKey(name);

        GridCacheQueueHeader header;

        if (create) {
            header = new GridCacheQueueHeader(GridUuid.randomUuid(), cap, colloc, 0, 0, null);

            GridCacheQueueHeader old = queueHdrView.putIfAbsent(key, header);

            if (old != null) {
                if (old.capacity() != cap || old.collocated() != colloc)
                    throw new GridException("Failed to create queue, queue with the same name but different " +
                        "configuration already exists [name=" + name + ']');

                header = old;
            }
        }
        else
            header = queueHdrView.get(key);

        if (header == null)
            return null;

        GridCacheQueueProxy queue = queuesMap.get(header.id());

        if (queue == null) {
            queue = new GridCacheQueueProxy(cctx, cctx.atomic() ? new GridAtomicCacheQueueImpl<>(name, header, cctx) :
                new GridTransactionalCacheQueueImpl<>(name, header, cctx));

            GridCacheQueueProxy old = queuesMap.putIfAbsent(header.id(), queue);

            if (old != null)
                queue = old;
        }

        return queue;
    }

    /**
     * Removes queue from cache.
     *
     * @param name Queue name.
     * @param batchSize Batch size.
     * @return Method returns {@code true} if queue has been removed and {@code false} if it's not cached.
     * @throws GridException If removing failed.
     */
    public final boolean removeQueue(final String name, final int batchSize) throws GridException {
        waitInitialization();

        checkSupportsQueue();

        if (cctx.atomic())
            return removeQueue0(name);

        return CU.outTx(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                return removeQueue0(name);
            }
        }, cctx);
    }

    /**
     * @param name Queue name.
     * @return {@code True} if queue was removed.
     * @throws GridException If failed.
     */
    private boolean removeQueue0(String name) throws GridException {
        // Remove only header, queue items are removed by QueueCleanupWorkers.
        return queueHdrView.removex(new GridCacheQueueHeaderKey(name));
    }

    /**
     * Gets or creates count down latch. If count down latch is not found in cache,
     * it is created using provided name and count parameter.
     * <p>
     *
     * @param name Name of the latch.
     * @param cnt Initial count.
     * @param autoDel {@code True} to automatically delete latch from cache when
     *      its count reaches zero.
     * @param create If {@code true} latch will be created in case it is not in cache,
     *      if it is {@code false} all parameters except {@code name} are ignored.
     * @return Count down latch for the given name or {@code null} if it is not found and
     *      {@code create} is false.
     * @throws GridException If operation failed.
     */
    public GridCacheCountDownLatch countDownLatch(final String name, final int cnt, final boolean autoDel,
        final boolean create) throws GridException {
        A.ensure(cnt >= 0, "count can not be negative");

        waitInitialization();

        checkTransactionalWithNear();

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheCountDownLatch latch = cast(dsMap.get(key), GridCacheCountDownLatch.class);

            if (latch != null)
                return latch;

            return CU.outTx(new Callable<GridCacheCountDownLatch>() {
                    @Override public GridCacheCountDownLatch call() throws Exception {
                        try (GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                            GridCacheCountDownLatchValue val = cast(dsView.get(key),
                                GridCacheCountDownLatchValue.class);

                            // Check that count down hasn't been created in other thread yet.
                            GridCacheCountDownLatchEx latch = cast(dsMap.get(key), GridCacheCountDownLatchEx.class);

                            if (latch != null) {
                                assert val != null;

                                return latch;
                            }

                            if (val == null && !create)
                                return null;

                            if (val == null) {
                                val = new GridCacheCountDownLatchValue(cnt, autoDel);

                                dsView.putx(key, val);
                            }

                            latch = new GridCacheCountDownLatchImpl(name, val.get(), val.initialCount(),
                                val.autoDelete(), key, cntDownLatchView, cctx);

                            dsMap.put(key, latch);

                            tx.commit();

                            return latch;
                        }
                        catch (Error | Exception e) {
                            dsMap.remove(key);

                            U.error(log, "Failed to create count down latch: " + name, e);

                            throw e;
                        }
                    }
                }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get count down latch by name: " + name, e);
        }
    }

    /**
     * Removes count down latch from cache.
     *
     * @param name Name of the latch.
     * @return Count down latch for the given name.
     * @throws GridException If operation failed.
     */
    public boolean removeCountDownLatch(final String name) throws GridException {
        waitInitialization();

        checkTransactionalWithNear();

        try {
            return CU.outTx(
                new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        GridCacheInternal key = new GridCacheInternalKeyImpl(name);

                        try (GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                            // Check correctness type of removable object.
                            GridCacheCountDownLatchValue val =
                                cast(dsView.get(key), GridCacheCountDownLatchValue.class);

                            if (val != null) {
                                if (val.get() > 0) {
                                    throw new GridException("Failed to remove count down latch " +
                                        "with non-zero count: " + val.get());
                                }

                                dsView.removex(key);

                                tx.commit();
                            }
                            else
                                tx.setRollbackOnly();

                            return val != null;
                        }
                        catch (Error | Exception e) {
                            U.error(log, "Failed to remove data structure: " + key, e);

                            throw e;
                        }
                    }
                },
                cctx
            );
        }
        catch (Exception e) {
            throw new GridException("Failed to remove count down latch by name: " + name, e);
        }
    }

    /**
     * Remove internal entry by key from cache.
     *
     * @param key Internal entry key.
     * @param cls Class of object which will be removed. If cached object has different type exception will be thrown.
     * @return Method returns true if sequence has been removed and false if it's not cached.
     * @throws GridException If removing failed or class of object is different to expected class.
     */
    private <R> boolean removeInternal(final GridCacheInternal key, final Class<R> cls) throws GridException {
        return CU.outTx(
            new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    try (GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                        // Check correctness type of removable object.
                        R val = cast(dsView.get(key), cls);

                        if (val != null) {
                            dsView.removex(key);

                            tx.commit();
                        }
                        else
                            tx.setRollbackOnly();

                        return val != null;
                    }
                    catch (Error | Exception e) {
                        U.error(log, "Failed to remove data structure: " + key, e);

                        throw e;
                    }
                }
            },
            cctx
        );
    }

    /**
     * Transaction committed callback for transaction manager.
     *
     * @param tx Committed transaction.
     */
    public void onTxCommitted(GridCacheTxEx<K, V> tx) {
        if (!cctx.isDht() && tx.internal() && (!cctx.isColocated() || cctx.isReplicated())) {
            try {
                waitInitialization();
            }
            catch (GridException e) {
                U.error(log, "Failed to wait for manager initialization.", e);

                return;
            }

            Collection<GridCacheTxEntry<K, V>> entries = tx.writeEntries();

            if (log.isDebugEnabled())
                log.debug("Committed entries: " + entries);

            for (GridCacheTxEntry<K, V> entry : entries) {
                // Check updated or created GridCacheInternalKey keys.
                if ((entry.op() == CREATE || entry.op() == UPDATE) && entry.key() instanceof GridCacheInternalKey) {
                    GridCacheInternal key = (GridCacheInternal)entry.key();

                    if (entry.value() instanceof GridCacheCountDownLatchValue) {
                        // Notify latch on changes.
                        GridCacheRemovable latch = dsMap.get(key);

                        GridCacheCountDownLatchValue val = (GridCacheCountDownLatchValue)entry.value();

                        if (latch instanceof GridCacheCountDownLatchEx) {
                            GridCacheCountDownLatchEx latch0 = (GridCacheCountDownLatchEx)latch;

                            latch0.onUpdate(val.get());

                            if (val.get() == 0 && val.autoDelete()) {
                                entry.cached().markObsolete(cctx.versions().next());

                                dsMap.remove(key);

                                latch.onRemoved();
                            }
                        }
                        else if (latch != null) {
                            U.error(log, "Failed to cast object " +
                                "[expected=" + GridCacheCountDownLatch.class.getSimpleName() +
                                ", actual=" + latch.getClass() + ", value=" + latch + ']');
                        }
                    }
                }

                // Check deleted GridCacheInternal keys.
                if (entry.op() == DELETE && entry.key() instanceof GridCacheInternal) {
                    GridCacheInternal key = (GridCacheInternal)entry.key();

                    // Entry's val is null if entry deleted.
                    GridCacheRemovable obj = dsMap.remove(key);

                    if (obj != null)
                        obj.onRemoved();
                }
            }
        }
    }

    /**
     * @throws GridException If thread is interrupted or manager
     *     was not successfully initialized.
     */
    private void waitInitialization() throws GridException {
        if (initLatch.getCount() > 0)
            U.await(initLatch);

        if (!initFlag)
            throw new GridException("DataStructures manager was not properly initialized for cache: " +
                cctx.cache().name());
    }

    /**
     * @return {@code True} if cache is transactional with near cache enabled.
     */
    private boolean transactionalWithNear() {
        return cctx.transactional() && (CU.isNearEnabled(cctx) || cctx.isReplicated() || cctx.isLocal());
    }


    /**
     * @return {@code True} if {@link GridCacheQueue} can be used with current cache configuration.
     */
    private boolean supportsQueue() {
        return !(cctx.atomic() && !cctx.isLocal() && cctx.config().getAtomicWriteOrderMode() == CLOCK);
    }

    /**
     * @throws GridException If {@link GridCacheQueue} can not be used with current cache configuration.
     */
    private void checkSupportsQueue() throws GridException {
        if (cctx.atomic() && !cctx.isLocal() && cctx.config().getAtomicWriteOrderMode() == CLOCK)
            throw new GridException("GridCacheQueue can not be used with ATOMIC cache with CLOCK write order mode" +
                " (change write order mode to PRIMARY in configuration)");
    }

    /**
     * @throws GridException If cache is not transactional with near cache enabled.
     */
    private void checkTransactionalWithNear() throws GridException {
        if (cctx.atomic())
            throw new GridException("Data structures require GridCacheAtomicityMode.TRANSACTIONAL atomicity mode " +
                "(change atomicity mode from ATOMIC to TRANSACTIONAL in configuration)");

        if (!cctx.isReplicated() && !cctx.isLocal() && !CU.isNearEnabled(cctx))
            throw new GridException("Cache data structures can not be used with near cache disabled on cache: " +
                cctx.cache().name());
    }

    /**
     * Tries to cast the object to expected type.
     *
     * @param obj Object which will be casted.
     * @param cls Class
     * @param <R> Type of expected result.
     * @return Object has casted to expected type.
     * @throws GridException If {@code obj} has different to {@code cls} type.
     */
    @SuppressWarnings("unchecked")
    @Nullable private <R> R cast(@Nullable Object obj, Class<R> cls) throws GridException {
        if (obj == null)
            return null;

        if (cls.isInstance(obj))
            return (R)obj;
        else
            throw new GridException("Failed to cast object [expected=" + cls + ", actual=" + obj.getClass() + ']');
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Data structure manager memory stats [grid=" + cctx.gridName() + ", cache=" + cctx.name() + ']');
        X.println(">>>   dsMapSize: " + dsMap.size());
    }

    /**
     * Predicate for queue continuous query.
     */
    private static class QueueHeaderPredicate implements GridBiPredicate, Externalizable {
        /**
         * Required by {@link Externalizable}.
         */
        public QueueHeaderPredicate() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Object key, Object val) {
            return key instanceof GridCacheQueueHeaderKey;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) {
            // No-op.
        }
    }

    /**
     * Worker removing orphaned queue items.
     */
    private class QueueCleanupWorker extends GridWorker {
        /** */
        private final int workersCnt;

        /** */
        private final int idx;

        /** */
        private boolean hasJob;

        /**
         * @param workersCnt Total number of worker threads.
         * @param idx Index of this worker (used to divide keys between multiple workers).
         */
        private QueueCleanupWorker(int workersCnt, int idx) {
            super(cctx.gridName(), "queue-cleanup-worker-" + idx, log);

            this.workersCnt = workersCnt;
            this.idx = idx;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            final int BATCH_SIZE = 10;

            final long cleanupFreq = cctx.config().getCacheQueueCleanupFrequency();

            while (!isCancelled()) {
                try {
                    synchronized (this) {
                        long endWait = U.currentTimeMillis() + cleanupFreq;

                        long waitTime;

                        while (!hasJob && (waitTime = endWait - U.currentTimeMillis()) > 0)
                            wait(waitTime);

                        hasJob = false;
                    }

                    GridCacheAdapter cache = cctx.cache();

                    Map<GridUuid, GridCacheQueueHeader> aliveQueues = new GridLeanMap<>();

                    Collection<GridUuid> deadQueues = new GridLeanSet<>();

                    Collection<GridCacheQueueItemKey> rmvKeys = null;

                    int cnt = 0;

                    Set<GridCacheEntryEx<Object, Object>> entries = cache.map().allEntries0();

                    for (GridCacheEntryEx<Object, Object> entry : entries) {
                        if (!processEntry(entry))
                            continue;

                        GridCacheQueueItemKey key = (GridCacheQueueItemKey)entry.key();

                        boolean rmv;

                        if (deadQueues.contains(key.queueId()))
                            rmv = true;
                        else if (aliveQueues.containsKey(key.queueId()))
                            rmv = removeItem(aliveQueues.get(key.queueId()), key);
                        else {
                            GridCacheQueueHeader header = (GridCacheQueueHeader)cache.get(
                                new GridCacheQueueHeaderKey(key.queueName()));

                            if (header == null || !header.id().equals(key.queueId())) {
                                deadQueues.add(key.queueId());

                                rmv = true;
                            }
                            else {
                                aliveQueues.put(header.id(), header);

                                rmv = removeItem(header, key);
                            }
                        }

                        if (rmv) {
                            if (log.isDebugEnabled())
                                log.debug("Found orphaned queue item [key=" + key + ", cache=" + cctx.name() + ']');

                            if (rmvKeys == null)
                                rmvKeys = new ArrayList<>(BATCH_SIZE);

                            rmvKeys.add(key);

                            if (rmvKeys.size() == BATCH_SIZE) {
                                cache.removeAll(rmvKeys);

                                rmvKeys.clear();
                            }
                        }

                        if (++cnt % QUEUE_CLEANUP_THROTTLE_ITEMS == 0) {
                            if (isCancelled())
                                return;

                            U.sleep(QUEUE_CLEANUP_THROTTLE_DELAY);
                        }
                    }

                    if (!F.isEmpty(rmvKeys))
                        cache.removeAll(rmvKeys);
                }
                catch (GridInterruptedException ignore) {
                    return;
                }
                catch (GridException e) {
                    U.error(log, "Failed to cleanup orphaned queue items [cache=" + cctx.name() + ']', e);
                }
            }
        }

        /**
         * Wakes up worker.
         */
        private void wakeUp() {
            synchronized (this) {
                hasJob = true;

                notifyAll();
            }
        }

        /**
         * @param e Cache entry.
         * @return {@code True} if this worker should process given entry.
         */
        @SuppressWarnings("IfMayBeConditional")
        private boolean processEntry(GridCacheEntryEx<Object, Object> e) {
            if (!(e.key() instanceof GridCacheQueueItemKey))
                return false;

            if (cctx.isLocal())
                return e.key().hashCode() % workersCnt == idx;
            else
                return cctx.affinity().primary(cctx.localNode(), e.partition()) && e.partition() % workersCnt == idx;
        }

        /**
         * @param header Queue header.
         * @param key Item key.
         * @return {@code True} if item is orphaned and should be removed.
         */
        @SuppressWarnings("SimplifiableIfStatement")
        private boolean removeItem(GridCacheQueueHeader header, GridCacheQueueItemKey key) {
            return cctx.transactional() ? key.index() < header.head() : key.index() < (header.head() - 1000);
        }
    }
}
