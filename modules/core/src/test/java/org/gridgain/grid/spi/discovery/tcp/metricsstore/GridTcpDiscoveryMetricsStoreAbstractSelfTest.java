/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.metricsstore;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Metrics store abstract test.
 */
public abstract class GridTcpDiscoveryMetricsStoreAbstractSelfTest<T extends GridTcpDiscoveryMetricsStore>
    extends GridCommonAbstractTest {
    /** */
    private static final int STORE_SIZE = 100;

    /** */
    private static final int THREAD_CNT = 10;

    /** */
    private static final int ITER_CNT = 50;

    /** */
    private final UUID ids[];

    /** */
    protected final T store;

    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    @SuppressWarnings({"AbstractMethodCallInConstructor", "OverriddenMethodCallDuringObjectConstruction"})
    protected GridTcpDiscoveryMetricsStoreAbstractSelfTest() throws Exception {
        super(false);

        assert STORE_SIZE > 2 : "Store size should be greater, than 2.";

        ids = new UUID[STORE_SIZE];

        for (int i = 0; i < STORE_SIZE; i++)
            ids[i] = UUID.randomUUID();

        store = metricsStore();

        getTestResources().inject(store);

        // Clean store.
        Collection<UUID> allNodeIds = store.allNodeIds();

        if (!allNodeIds.isEmpty())
            store.removeMetrics(allNodeIds);

        assert store.allNodeIds().isEmpty();
    }

    /**
     * @throws Exception If any error occurs.
     */
    @SuppressWarnings("ErrorNotRethrown")
    public void testStore() throws Exception {
        for (int i = 0; i < STORE_SIZE; i++)
            store.updateLocalMetrics(ids[i], new GridDiscoveryMetricsAdapter());

        List<UUID> nodeIds = Arrays.asList(ids);

        Map<UUID,GridNodeMetrics> metrics = store.metrics(nodeIds);

        assert metrics.size() == STORE_SIZE;
        assert metrics.keySet().contains(ids[0]) && metrics.keySet().contains(ids[1]);

        Collection<UUID> allNodeIds = store.allNodeIds();

        assert allNodeIds.containsAll(nodeIds);
        assert allNodeIds.size() == ids.length;

        for (int i = 0; i < 3; i++) {
            try {
                store.removeMetrics(nodeIds);

                assert store.allNodeIds().isEmpty();
            }
            catch (Error e) {
                if (i == 2)
                    throw e;

                U.warn(log, "Store is not empty (will retry in 1000 ms).");

                U.sleep(1000);
            }
        }
    }

    /**
     * @throws Exception if any error occurs.
     */
    public void testStoreMultiThreaded() throws Exception {
        GridFuture fut1 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                for (int j = 0; j < ITER_CNT; j++)
                    for (int i = 0; i < STORE_SIZE; i++)
                        store.updateLocalMetrics(ids[i], new GridDiscoveryMetricsAdapter());

                return null;
            }
        }, THREAD_CNT, "writers");

        GridFuture fut2 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                for (int j = 0; j < ITER_CNT; j++)
                    store.metrics(Arrays.asList(ids)).size();

                return null;
            }
        }, THREAD_CNT, "readers");

        GridFuture fut3 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                for (int j = 0; j < ITER_CNT; j++)
                    for (int i = 0; i < STORE_SIZE; i++)
                        store.removeMetrics(Arrays.asList(ids[i]));

                return null;
            }
        }, THREAD_CNT, "removers");

        fut1.get();
        fut2.get();
        fut3.get();

        // Clean store.
        Collection<UUID> allNodeIds = store.allNodeIds();

        if (!allNodeIds.isEmpty())
            store.removeMetrics(allNodeIds);
    }

    /**
     * Creates and initializes metrics store.
     *
     * @return Metrics store.
     * @throws Exception If any error occurs.
     */
    protected abstract T metricsStore() throws Exception;
}
