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
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;

/**
 * Test store without batch.
 */
public abstract class GridCacheBasicStoreNoBatchAbstractTest extends GridCommonAbstractTest {
    /** Cache store. */
    private static final GridCacheTestStore store = new GridCacheTestStore();

    /** Constructs a test. */
    protected GridCacheBasicStoreNoBatchAbstractTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        store.resetTimestamp();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridCache<Object, Object> cache = cache();

        if (cache != null)
            cache.removeAll(F.<GridCacheEntry<Object, Object>>alwaysTrue());

        store.reset();
    }

    /** @return Cache mode. */
    protected abstract GridCacheMode cacheMode();

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration c = super.getConfiguration();

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());

        cc.setStore(store);

        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cc.setAtomicityMode(atomicityMode());

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected GridCacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** @throws Exception If test fails. */
    public void testWriteThrough() throws Exception {
        GridCache<Integer, String> cache = cache();

        Map<Integer, String> map = store.getMap();

        assert map.isEmpty();

        GridCacheTx tx = atomicityMode() == TRANSACTIONAL ? cache.txStart() : null;

        try {
            for (int i = 1; i <= 10; i++) {
                cache.put(i, Integer.toString(i));

                checkLastMethod("put");
            }

            if (tx != null) {
                tx.commit();

                checkLastMethod("put");
            }
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache.size() == 10;

        for (int i = 1; i <= 10; i++) {
            String val = map.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        store.resetLastMethod();

        tx = atomicityMode() == TRANSACTIONAL ? cache.txStart() : null;

        try {
            for (int i = 1; i <= 10; i++) {
                String val = cache.remove(i);

                checkLastMethod("remove");

                assert val != null;
                assert val.equals(Integer.toString(i));
            }

            if (tx != null) {
                tx.commit();

                checkLastMethod("remove");
            }
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert map.isEmpty();
    }

    /** @throws Exception If test failed. */
    public void testReadThrough() throws Exception {
        GridCache<Integer, String> cache = cache();

        Map<Integer, String> map = store.getMap();

        assert map.isEmpty();

        if (atomicityMode() == TRANSACTIONAL) {
            try (GridCacheTx tx = cache.txStart()) {
                for (int i = 1; i <= 10; i++) {
                    cache.put(i, Integer.toString(i));

                    checkLastMethod("put");
                }

                tx.commit();

                checkLastMethod("put");
            }
        }
        else {
            for (int i = 1; i <= 10; i++) {
                cache.put(i, Integer.toString(i));

                checkLastMethod("put");
            }
        }

        for (int i = 1; i <= 10; i++) {
            String val = map.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        cache.clearAll();

        assert cache.isEmpty();

        assert map.size() == 10;

        for (int i = 1; i <= 10; i++) {
            // Read through.
            String val = cache.get(i);

            checkLastMethod("load");

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        assert cache.size() == 10;

        cache.clearAll();

        assert cache.isEmpty();

        assert map.size() == 10;

        Collection<Integer> keys = new ArrayList<>();

        for (int i = 1; i <= 10; i++)
            keys.add(i);

        // Read through.
        Map<Integer, String> vals = cache.getAll(keys);

        checkLastMethod("loadAll");

        assert vals != null;
        assert vals.size() == 10 : "Invalid values size: " + vals.size();

        for (int i = 1; i <= 10; i++) {
            String val = vals.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        // Write through.
        cache.removeAll(keys);

        checkLastMethod("removeAll");

        assert cache.isEmpty();
        assert cache.isEmpty();

        assert map.isEmpty();
    }

    /** @param mtd Expected last method value. */
    private void checkLastMethod(String mtd) {
        String lastMtd = store.getLastMethod();

        if (mtd == null)
            assert lastMtd == null : "Last method must be null: " + lastMtd;
        else {
            assert lastMtd != null : "Last method must be not null";
            assert lastMtd.equals(mtd) : "Last method does not match [expected=" + mtd + ", lastMtd=" + lastMtd + ']';
        }
    }
}
