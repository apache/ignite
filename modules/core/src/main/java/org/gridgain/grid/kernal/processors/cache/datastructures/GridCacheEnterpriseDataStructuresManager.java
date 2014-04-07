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
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;

/**
 * Manager of data structures.
 */
public final class GridCacheEnterpriseDataStructuresManager<K, V> extends GridCacheDataStructuresManager<K, V> {
    /** Initial capacity. */
    private static final int INITIAL_CAPACITY = 10;

    /** */
    private static final AtomicLong setIterId = new AtomicLong();

    /** */
    private static final int SET_ITER_PAGE_SIZE = 3;

    /** Cache contains only {@code GridCacheInternal,GridCacheInternal}. */
    private GridCacheProjection<GridCacheInternal, GridCacheInternal> dsView;

    /** Internal storage of all dataStructures items (sequence, queue , atomic long etc.). */
    private final ConcurrentMap<GridCacheInternal, GridCacheRemovable> dsMap;

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
    private GridCacheProjection<GridCacheInternalKey, GridCacheQueueHeader> queueHdrView;

    /** Cache contains only entry {@code GridCacheQueueItem}  */
    private GridCacheProjection<GridCacheQueueItemKey, GridCacheQueueItem> queueItemView;

    /** Query factory. */
    private GridCacheQueueQueryFactory queueQryFactory;

    /** Init latch. */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Init flag. */
    private boolean initFlag;

    /** Queue busy lock. */
    private final GridSpinBusyLock queueBusyLock = new GridSpinBusyLock();

    /** Queue delete worker. */
    private QueueDeleteWorker queueDelWorker;

    /** Queue remove candidates identified during preload. */
    private final BlockingQueue<GridCacheInternal> queueDelCands = new LinkedBlockingQueue<>();

    /** */
    private ConcurrentMap<GridUuid, GridConcurrentHashSet<GridCacheSetItemKey>> setData =
        new ConcurrentHashMap8<>(INITIAL_CAPACITY);

    /** */
    private ConcurrentMap<Long, GridCacheSetIterator> setIterMap = new ConcurrentHashMap8<>();

    /** */
    private ConcurrentMap<SetIteratorKey, Iterator<GridCacheSetItemKey>> locSetIterMap = new ConcurrentHashMap8<>();

    /**
     * Default constructor.
     */
    public GridCacheEnterpriseDataStructuresManager() {
        dsMap = new ConcurrentHashMap8<>(INITIAL_CAPACITY);
    }

    /** {@inheritDoc} */
    @Override protected void start0() {
        cctx.io().addHandler(GridCacheSetDataRequest.class, new CI2<UUID, GridCacheSetDataRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridCacheSetDataRequest<K, V> req) {
                processSetIteratorRequest(nodeId, req);
            }
        });

        cctx.io().addHandler(GridCacheSetDataResponse.class, new CI2<UUID, GridCacheSetDataResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridCacheSetDataResponse<K, V> req) {
                processSetIteratorResponse(nodeId, req);
            }
        });
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void onKernalStart0() {
        try {
            if (!cctx.isColocated() || cctx.isReplicated()) {
                assert !cctx.isDht();

                dsView = cctx.cache().<GridCacheInternal, GridCacheInternal>projection
                    (GridCacheInternal.class, GridCacheInternal.class).flagsOn(CLONE);

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

                queueHdrView = cctx.cache().<GridCacheInternalKey, GridCacheQueueHeader>projection
                    (GridCacheInternalKey.class, GridCacheQueueHeader.class).flagsOn(CLONE);

                queueItemView = cctx.cache().<GridCacheQueueItemKey, GridCacheQueueItem>projection
                    (GridCacheQueueItemKey.class, GridCacheQueueItem.class).flagsOn(CLONE);

                queueQryFactory = new GridCacheQueueQueryFactory(cctx);

                initFlag = true;
            }

            queueDelWorker = new QueueDeleteWorker(cctx.kernalContext().gridName(), "queue-del-worker", log);
        }
        finally {
            initLatch.countDown();
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        queueBusyLock.block();

        for (GridCacheRemovable entry : dsMap.values()) {
            if (entry instanceof GridCacheQueue) {
                try {
                    GridCacheQueueEx queue = cast(entry, GridCacheQueueEx.class);

                    queue.onRemoved();
                }
                catch (GridException e) {
                    U.warn(log, "Failed to remove queue entry: " + e);
                }
            }
        }

        if (queueDelWorker != null) {
            queueDelWorker.cancel();

            try {
                queueDelWorker.join();
            }
            catch (InterruptedException ignore) {
                U.warn(log, "Interrupted while waiting for queue delete worker to stop.");

                Thread.currentThread().interrupt();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onPartitionsChange() {
        for (Map.Entry<GridCacheInternal, GridCacheRemovable> e : dsMap.entrySet()) {
            final GridCacheRemovable v = e.getValue();

            if (v instanceof GridCacheQueue) {
                final GridCacheInternal key = e.getKey();

                GridCacheProjectionEx<GridCacheInternal, GridCacheInternal> cache =
                    (GridCacheProjectionEx<GridCacheInternal, GridCacheInternal>)cctx.cache();

                // Re-initialize near cache.
                GridFuture<GridCacheInternal> fut = cache.getForcePrimaryAsync(key);

                fut.listenAsync(new CI1<GridFuture<GridCacheInternal>>() {
                    @Override public void apply(GridFuture<GridCacheInternal> f) {
                        if (queueBusyLock.enterBusy()) {
                            try {
                                GridCacheQueueHeader hdr = cast(f.get(), GridCacheQueueHeader.class);

                                if (hdr != null) {
                                    GridCacheQueueEx queue = cast(v, GridCacheQueueEx.class);

                                    queue.onHeaderChanged(hdr);
                                }
                                else if (v != null)
                                    queueDelCands.add(key);
                            }
                            catch (GridException ex) {
                                U.error(log, "Failed to synchronize queue state (will invalidate the queue): " + v, ex);

                                v.onInvalid(ex);
                            }
                            finally {
                                queueBusyLock.leaveBusy();
                            }
                        }
                        else
                            U.warn(log, "Partition change callback is ignored because grid is stopping.");
                    }
                });
            }
        }
    }

    /**
     * @throws GridException If {@link GridCacheAtomicityMode#ATOMIC} mode.
     */
    private void checkAtomicity() throws GridException {
        if (cctx.config().getAtomicityMode() == GridCacheAtomicityMode.ATOMIC)
            throw new GridException("Data structures require GridCacheAtomicityMode.TRANSACTIONAL atomicity mode " +
                "(change atomicity mode from ATOMIC to TRANSACTIONAL in configuration)");
    }

    /** {@inheritDoc} */
    @Override public final GridCacheAtomicSequence sequence(final String name, final long initVal,
        final boolean create) throws GridException {
        waitInitialization();

        checkAtomicity();

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheAtomicSequence val = cast(dsMap.get(key), GridCacheAtomicSequence.class);

            if (val != null)
                return val;

            return CU.outTx(new Callable<GridCacheAtomicSequence>() {
                    @Override public GridCacheAtomicSequence call() throws Exception {
                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
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
                        } finally {
                            tx.close();
                        }
                    }
                }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get sequence by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean removeSequence(String name) throws GridException {
        waitInitialization();

        checkAtomicity();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicSequenceValue.class);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove sequence by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public final GridCacheAtomicLong atomicLong(final String name, final long initVal,
        final boolean create) throws GridException {
        waitInitialization();

        checkAtomicity();

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheAtomicLong atomicLong = cast(dsMap.get(key), GridCacheAtomicLong.class);

            if (atomicLong != null)
                return atomicLong;

            return CU.outTx(new Callable<GridCacheAtomicLong>() {
                    @Override public GridCacheAtomicLong call() throws Exception {
                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
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
                        } finally {
                            tx.close();
                        }
                    }
                }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get atomic long by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean removeAtomicLong(String name) throws GridException {
        waitInitialization();

        checkAtomicity();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicLongValue.class);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove atomic long by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override @SuppressWarnings("unchecked")
    public final <T> GridCacheAtomicReference<T> atomicReference(final String name, final T initVal,
        final boolean create) throws GridException {
        waitInitialization();

        checkAtomicity();

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheAtomicReference atomicRef = cast(dsMap.get(key), GridCacheAtomicReference.class);

            if (atomicRef != null)
                return atomicRef;

            return CU.outTx(new Callable<GridCacheAtomicReference<T>>() {
                    @Override public GridCacheAtomicReference<T> call() throws Exception {
                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
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
                        } finally {
                            tx.close();
                        }
                    }
                }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get atomic reference by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean removeAtomicReference(String name) throws GridException {
        waitInitialization();

        checkAtomicity();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicReferenceValue.class);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove atomic reference by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public final <T, S> GridCacheAtomicStamped<T, S> atomicStamped(final String name, final T initVal,
        final S initStamp, final boolean create) throws GridException {
        waitInitialization();

        checkAtomicity();

        final GridCacheInternalKeyImpl key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheAtomicStamped atomicStamped = cast(dsMap.get(key), GridCacheAtomicStamped.class);

            if (atomicStamped != null)
                return atomicStamped;

            return CU.outTx(new Callable<GridCacheAtomicStamped<T, S>>() {
                    @Override public GridCacheAtomicStamped<T, S> call() throws Exception {
                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
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
                        } finally {
                            tx.close();
                        }
                    }
                }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get atomic stamped by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean removeAtomicStamped(String name) throws GridException {
        waitInitialization();

        checkAtomicity();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicStampedValue.class);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove atomic stamped by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public final <T> GridCacheQueue<T> queue(final String name, final int cap, boolean colloc,
        final boolean create) throws GridException {
        A.ensure(cap > 0, "cap > 0");

        waitInitialization();

        checkAtomicity();

        // Non collocated mode enabled only for PARTITIONED cache.
        final boolean collocMode = cctx.cache().configuration().getCacheMode() != PARTITIONED
            || colloc;

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheQueue queue = cast(dsMap.get(key), GridCacheQueue.class);

            if (queue != null)
                return queue;

            return CU.outTx(new Callable<GridCacheQueue<T>>() {
                    @Override public GridCacheQueue<T> call() throws Exception {
                        if (queueBusyLock.enterBusy()) {
                            try {
                                GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                                try {
                                    GridCacheQueueHeader hdr = cast(dsView.get(key), GridCacheQueueHeader.class);

                                    GridCacheQueueEx queue = cast(dsMap.get(key), GridCacheQueueEx.class);

                                    if (queue != null) {
                                        assert hdr != null : "Failed to find queue header in cache: " + queue;

                                        return queue;
                                    }

                                    if (hdr == null) {
                                        if (!create)
                                            return null;

                                        hdr = new GridCacheQueueHeader(name, cap, collocMode);

                                        dsView.putx(key, hdr);
                                    }

                                    queue = new GridCacheQueueImpl(name, hdr, key, cctx, queueHdrView, queueItemView,
                                        queueQryFactory);

                                    GridCacheRemovable prev = dsMap.put(key, queue);

                                    assert prev == null;

                                    tx.commit();

                                    return queue;
                                }
                                catch (Error | Exception e) {
                                    dsMap.remove(key);

                                    U.error(log, "Failed to create queue: " + name, e);

                                    throw e;
                                } finally {
                                    tx.close();
                                }
                            }
                            finally {
                                queueBusyLock.leaveBusy();
                            }
                        }
                        else
                            throw new GridException("Failed to get queue because grid is stopping.");
                    }
                }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get queue by name :" + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean removeQueue(String name, int batchSize) throws GridException {
        waitInitialization();

        checkAtomicity();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            GridCacheQueueEx queue = cast(dsMap.get(key), GridCacheQueueEx.class);

            return queue != null && queue.removeQueue(batchSize);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove queue by name :" + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheCountDownLatch countDownLatch(final String name, final int cnt, final boolean autoDel,
        final boolean create) throws GridException {
        A.ensure(cnt >= 0, "count can not be negative" );

        waitInitialization();

        checkAtomicity();

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheCountDownLatch latch = cast(dsMap.get(key), GridCacheCountDownLatch.class);

            if (latch != null)
                return latch;

            return CU.outTx(new Callable<GridCacheCountDownLatch>() {
                    @Override public GridCacheCountDownLatch call() throws Exception {
                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
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
                        } finally {
                            tx.close();
                        }
                    }
                }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get count down latch by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeCountDownLatch(final String name) throws GridException {
        waitInitialization();

        checkAtomicity();

        try {
            return CU.outTx(
                new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        GridCacheInternal key = new GridCacheInternalKeyImpl(name);

                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
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
                        } finally {
                            tx.close();
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

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> Set<T> set(String name, boolean create) throws GridException {
        waitInitialization();

        GridCacheSetHeaderKey key = new GridCacheSetHeaderKey(name);

        GridCacheSetHeader hdr;

        GridCacheAdapter cache = cctx.cache();

        if (create) {
            hdr = new GridCacheSetHeader(GridUuid.randomUuid());

            GridCacheSetHeader old = (GridCacheSetHeader)cache.putIfAbsent(key, hdr);

            if (old != null)
                hdr = old;
        }
        else
            hdr = (GridCacheSetHeader)cache.get(key);

        if (hdr == null)
            return null;

        return new GridCacheSet<>(cctx, name, hdr.setId());
    }

    /** {@inheritDoc} */
    @Override public boolean removeSet(String name) throws GridException {
        waitInitialization();

        GridCacheSetHeaderKey key = new GridCacheSetHeaderKey(name);

        GridCacheAdapter cache = cctx.cache();

        return cache.removex(key);
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
                    GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                    try {
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
                    } finally {
                        tx.close();
                    }
                }
            },
            cctx
        );
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Request.
     */
    @SuppressWarnings("unchecked")
    void processSetIteratorRequest(UUID nodeId, GridCacheSetDataRequest<K, V> req) {
        SetIteratorKey key = new SetIteratorKey(req.id(), nodeId);

        Iterator<GridCacheSetItemKey> it = locSetIterMap.get(key);

        if (it == null) {
            GridConcurrentHashSet<GridCacheSetItemKey> data = setData.get(req.setId());

            if (data == null) {
                if (log.isDebugEnabled())
                    log.debug("Received request for unknown set [req=" + req + ", nodeId=" + nodeId + ']');

                sendResponse(nodeId, new GridCacheSetDataResponse<K, V>(req.id(), Collections.emptyList(), true));

                return;
            }

            it = data.iterator();

            locSetIterMap.put(key, it);
        }

        List<Object> data = new ArrayList<>(req.pageSize());

        boolean last = false;

        boolean filterBackup = cctx.config().getBackups() > 0 || CU.isNearEnabled(cctx);

        GridCacheAffinityManager aff = cctx.affinity();

        while (data.size() < req.pageSize()) {
            GridCacheSetItemKey next = it.hasNext() ? it.next() : null;

            if (next == null) {
                last = true;

                break;
            }

            if (filterBackup && !aff.primary(cctx.localNode(), next, req.topologyVersion()))
                continue;

            data.add(next.item());
        }

        if (last)
            locSetIterMap.remove(key);

        sendResponse(nodeId, new GridCacheSetDataResponse<K, V>(req.id(), data, last));
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processSetIteratorResponse(UUID nodeId, GridCacheSetDataResponse<K, V> res) {
        res.nodeId(nodeId);

        GridCacheSetIterator iter = setIterMap.get(res.id());

        if (iter == null) {
            if (log.isDebugEnabled())
                log.debug("Received response for unknown set iterator [res=" + res + ']');
        }
        else
            iter.onResponse(res);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void sendResponse(UUID nodeId, GridCacheSetDataResponse<K, V> res) {
        if (nodeId.equals(cctx.localNodeId())) {
            processSetIteratorResponse(nodeId, res);

            return;
        }

        try {
            cctx.io().send(nodeId, res);
        }
        catch (GridTopologyException ignore) {
            if (log.isDebugEnabled())
                log.debug("");
        }
        catch (GridException e) {
            log.error("Failed to send iterator response", e);
        }
    }

    /**
     * @param key Set item key.
     * @param rmv {@code True} if item was removed.
     */
    private void onSetItemUpdated(GridCacheSetItemKey key, boolean rmv) {
        GridConcurrentHashSet<GridCacheSetItemKey> set = setData.get(key.setId());

        if (set == null) {
            GridConcurrentHashSet<GridCacheSetItemKey> old = setData.putIfAbsent(key.setId(),
                set = new GridConcurrentHashSet<>());

            if (old != null)
                set = old;
        }

        if (rmv)
            set.remove(key);
        else
            set.add(key);
    }

    /**
     * @param set Cache set.
     * @return Set iterator.
     * @throws GridException If failed.
     */
    <T> Iterator<T> setIterator(GridCacheSet set) throws GridException {
        long topVer = cctx.discovery().topologyVersion();

        long id = setIterId.incrementAndGet();

        Collection<UUID> nodeIds = new HashSet<>(F.viewReadOnly(CU.affinityNodes(cctx, topVer), F.node2id()));

        GridCacheSetDataRequest<K, V> req = new GridCacheSetDataRequest<>(id, set.id(), topVer, SET_ITER_PAGE_SIZE,
            false);

        GridCacheSetIterator<T> iter = new GridCacheSetIterator<>(set, nodeIds, req);

        setIterMap.put(id, iter);

        return iter;
    }

    /** {@inheritDoc} */
    @Override public void onTxCommitted(GridCacheTxEx<K, V> tx) {
        // TODO: check why called twice locally.
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
                if ((entry.op() == CREATE || entry.op() == UPDATE) && entry.key() instanceof GridCacheInternal) {
                    GridCacheInternal key = (GridCacheInternal)entry.key();

                    if (entry.value() instanceof GridCacheQueueHeader) {
                        if (queueBusyLock.enterBusy()) {
                            try {
                                // Notify queue on changes.
                                GridCacheRemovable queue = dsMap.get(key);

                                // If queue is used in current cache.
                                if (queue instanceof GridCacheQueueEx) {
                                    GridCacheQueueHeader hdr = (GridCacheQueueHeader)entry.value();

                                    ((GridCacheQueueEx)queue).onHeaderChanged(hdr);
                                }
                                else if (queue != null)
                                    U.error(log, "Failed to cast object [expected=" +
                                        GridCacheQueue.class.getSimpleName() + ", actual=" + queue.getClass() +
                                        ", value=" + queue + ']');
                            }
                            finally {
                                queueBusyLock.leaveBusy();
                            }
                        }
                        else
                            U.warn(log, "Ignored queue update from TX because grid is stopping.");
                    }
                    else if (entry.value() instanceof GridCacheCountDownLatchValue) {
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
                    else if (key instanceof GridCacheSetItemKey)
                        onSetItemUpdated((GridCacheSetItemKey) key, false);
                }

                // Check deleted GridCacheInternal keys.
                if (entry.op() == DELETE && entry.key() instanceof GridCacheInternal) {
                    GridCacheInternal key = (GridCacheInternal)entry.key();

                    if (key instanceof GridCacheSetItemKey)
                        onSetItemUpdated((GridCacheSetItemKey) key, true);

                    // Entry's val is null if entry deleted.
                    GridCacheRemovable obj = dsMap.remove(key);

                    if (obj != null)
                        obj.onRemoved();
                }
            }
        }
    }

    /**
     * Gets queue query factory.
     *
     * @return Queue query factory.
     */
    public GridCacheQueueQueryFactory queueQueryFactory() {
        return queueQryFactory;
    }

    /**
     * @throws GridException If thread is interrupted or manager
     *     was not successfully initialized.
     */
    private void waitInitialization() throws GridException {
        if (initLatch.getCount() > 0)
            U.await(initLatch);

        if (!initFlag) {
            if (cctx.isColocated())
                throw new GridException("Cache data structures can not be used with near cache disabled on cache: " +
                    cctx.cache().name());
            else
                throw new GridException("DataStructures manager was not properly initialized for cache: " +
                    cctx.cache().name());
        }
    }

    /**
     * Queue delete worker.
     */
    private class QueueDeleteWorker extends GridWorker {
        /**
         * Constructor.
         *
         * @param gridName Grid name.
         * @param name Worker name.
         * @param log Logger.
         */
        private QueueDeleteWorker(@Nullable String gridName, String name, GridLogger log) {
            super(gridName, name, log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            while (!isCancelled()) {
                GridCacheInternal key = queueDelCands.poll(1000, TimeUnit.MILLISECONDS);

                if (key != null) {
                    try {
                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
                            GridCacheQueueHeader hdr = cast(dsView.get(key), GridCacheQueueHeader.class);

                            GridCacheQueueEx queue = cast(dsMap.get(key), GridCacheQueueEx.class);

                            if (hdr == null && queue != null) {
                                queue.onRemoved();

                                dsMap.remove(key);
                            }

                            tx.commit();
                        }
                        finally {
                            tx.close();
                        }
                    }
                    catch (Exception e) {
                        U.warn(log, "Failed to check queue for existence: " + e);
                    }
                }
            }
        }
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
     * Set iterator key.
     */
    private static class SetIteratorKey {
        /** */
        private final long id;

        /** */
        private final UUID nodeId;

        /**
         * @param id Iterator ID.
         * @param nodeId Node ID.
         */
        private SetIteratorKey(long id, UUID nodeId) {
            this.id = id;
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SetIteratorKey that = (SetIteratorKey)o;

            return id == that.id && nodeId.equals(that.nodeId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = (int) (id ^ (id >>> 32));

            result = 31 * result + nodeId.hashCode();

            return result;
        }
    }
}
