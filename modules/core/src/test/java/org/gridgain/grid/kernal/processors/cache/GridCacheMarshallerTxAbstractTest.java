/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.transactions.GridCacheTxConcurrency.*;
import static org.apache.ignite.transactions.GridCacheTxIsolation.*;

/**
 * Test transaction with wrong marshalling.
 */
public abstract class GridCacheMarshallerTxAbstractTest extends GridCommonAbstractTest {
    /**
     * Wrong Externalizable class.
     */
    private static class GridCacheWrongValue implements Externalizable {
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            throw new NullPointerException("Expected exception.");
        }

        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            throw new NullPointerException("Expected exception.");
        }
    }

        /**
     * Wrong Externalizable class.
     */
    private static class GridCacheWrongValue1 {
        private int val1 = 8;
        private long val2 = 9;
    }

    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * Constructs a test.
     */
    protected GridCacheMarshallerTxAbstractTest() {
        super(true /* start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testValueMarshallerFail() throws Exception {
        String key = UUID.randomUUID().toString();
        String value = UUID.randomUUID().toString();
        String newValue = UUID.randomUUID().toString();

        String key2 = UUID.randomUUID().toString();
        GridCacheWrongValue1 wrongValue = new GridCacheWrongValue1();

        IgniteTx tx = grid().cache(null).txStart(PESSIMISTIC, REPEATABLE_READ);
        try {
            grid().cache(null).put(key, value);

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = grid().cache(null).txStart(PESSIMISTIC, REPEATABLE_READ);

        try {
            assert value.equals(grid().cache(null).get(key));

            grid().cache(null).put(key, newValue);

            grid().cache(null).put(key2, wrongValue);

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = grid().cache(null).txStart(PESSIMISTIC, REPEATABLE_READ);

        try {
            String locVal = (String)grid().cache(null).get(key);

            assert locVal != null;

            tx.commit();
        }
        finally {
            tx.close();
        }
    }
}
