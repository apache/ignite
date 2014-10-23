/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test store.
 */
public final class GridCacheTestStore implements GridCacheStore<Integer, String> {
    /** Store. */
    private final Map<Integer, String> map;

    /** Transactions. */
    private final Collection<GridCacheTx> txs = new GridConcurrentHashSet<>();

    /** Last method called. */
    private String lastMtd;;

    /** */
    private long ts = System.currentTimeMillis();

    /** {@link #load(GridCacheTx, Object)} method call counter .*/
    private AtomicInteger loadCnt = new AtomicInteger();

    /** {@link #put(GridCacheTx, Object, Object)} method call counter .*/
    private AtomicInteger putCnt = new AtomicInteger();

    /** {@link #putAll(GridCacheTx, Map)} method call counter .*/
    private AtomicInteger putAllCnt = new AtomicInteger();

    /** Flag indicating if methods of this store should fail. */
    private volatile boolean shouldFail;

    /** Configurable delay to simulate slow storage. */
    private int operationDelay;

    /**
     * @param map Underlying store map.
     */
    public GridCacheTestStore(Map<Integer, String> map) {
        this.map = map;
    }

    /**
     * Default constructor.
     */
    public GridCacheTestStore() {
        map = new ConcurrentHashMap<>();
    }

    /**
     * @return Underlying map.
     */
    public Map<Integer, String> getMap() {
        return Collections.unmodifiableMap(map);
    }

    /**
     * Sets a flag indicating if methods of this class should fail with {@link GridException}.
     *
     * @param shouldFail {@code true} if should fail.
     */
    public void setShouldFail(boolean shouldFail) {
        this.shouldFail = shouldFail;
    }

    /**
     * Sets delay that this store should wait on each operation.
     *
     * @param operationDelay If zero, no delay applied, positive value means
     *        delay in milliseconds.
     */
    public void setOperationDelay(int operationDelay) {
        assert operationDelay >= 0;

        this.operationDelay = operationDelay;
    }

    /**
     * @return Transactions.
     */
    public Collection<GridCacheTx> transactions() {
        return txs;
    }

    /**
     *
     * @return Last method called.
     */
    public String getLastMethod() {
        return lastMtd;
    }

    /**
     * @return Last timestamp.
     */
    public long getTimestamp() {
        return ts;
    }

    /**
     * @return Integer timestamp.
     */
    public int getStart() {
        return Math.abs((int)ts);
    }

    /**
     * Sets last method to <tt>null</tt>.
     */
    public void resetLastMethod() {
        lastMtd = null;
    }

    /**
     * Resets timestamp.
     */
    public void resetTimestamp() {
        ts = System.currentTimeMillis();
    }

    /**
     * Resets the store to initial state.
     */
    public void reset() {
        lastMtd = null;

        map.clear();

        loadCnt.set(0);
        putCnt.set(0);
        putAllCnt.set(0);

        ts = System.currentTimeMillis();

        txs.clear();
    }

    /**
     * @return Count of {@link #load(GridCacheTx, Object)} method calls since last reset.
     */
    public int getLoadCount() {
        return loadCnt.get();
    }

    /**
     * @return Count of {@link #put(GridCacheTx, Object, Object)} method calls since last reset.
     */
    public int getPutCount() {
        return putCnt.get();
    }

    /**
     * @return Count of {@link #putAll(GridCacheTx, Map)} method calls since last reset.
     */
    public int getPutAllCount() {
        return putAllCnt.get();
    }

    /** {@inheritDoc} */
    @Override public String load(GridCacheTx tx, Integer key) throws GridException {
        checkTx(tx);

        lastMtd = "load";

        checkOperation();

        loadCnt.incrementAndGet();

        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public void loadCache(GridBiInClosure<Integer, String> clo, Object[] args)
        throws GridException {
        lastMtd = "loadAllFull";

        checkOperation();

        int start = getStart();

        int cnt = (Integer)args[0];

        for (int i = start; i < start + cnt; i++) {
            map.put(i, Integer.toString(i));

            clo.apply(i, Integer.toString(i));
        }
    }

    /** {@inheritDoc} */
    @Override public void loadAll(GridCacheTx tx, Collection<? extends Integer> keys,
        GridBiInClosure<Integer, String> c) throws GridException {
        checkTx(tx);

        lastMtd = "loadAll";

        checkOperation();

        for (Integer key : keys) {
            String val = map.get(key);

            if (val != null)
                c.apply(key, val);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx tx, Integer key, String val)
        throws GridException {
        checkTx(tx);

        lastMtd = "put";

        checkOperation();

        map.put(key, val);

        putCnt.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void putAll(GridCacheTx tx, Map<? extends Integer, ? extends String> map)
        throws GridException {
        checkTx(tx);

        lastMtd = "putAll";

        checkOperation();

        this.map.putAll(map);

        putAllCnt.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void remove(GridCacheTx tx, Integer key) throws GridException {
        checkTx(tx);

        lastMtd = "remove";

        checkOperation();

        map.remove(key);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(GridCacheTx tx, Collection<? extends Integer> keys)
        throws GridException {
        checkTx(tx);

        lastMtd = "removeAll";

        checkOperation();

        for (Integer key : keys)
            map.remove(key);
    }

    /** {@inheritDoc} */
    @Override public void txEnd(GridCacheTx tx, boolean commit) {
        // No-op.
    }

    /**
     * Checks the flag and throws exception if it is set. Checks operation delay and sleeps
     * for specified amount of time, if needed.
     *
     * @throws GridException Always if flag is set.
     */
    private void checkOperation() throws GridException {
        if (shouldFail)
            throw new GridException("Store exception.");

        if (operationDelay > 0)
            U.sleep(operationDelay);
    }

    /**
     * @param tx Checks transaction.
     * @throws GridException If transaction is incorrect.
     */
    private void checkTx(GridCacheTx tx) throws GridException {
        if (tx == null)
            return;

        txs.add(tx);

        GridCacheTxEx tx0 = (GridCacheTxEx)tx;

        if (!tx0.local())
            throw new GridException("Tx is not local: " + tx);

        if (tx0.dht())
            throw new GridException("Tx is DHT: " + tx);
    }
}
