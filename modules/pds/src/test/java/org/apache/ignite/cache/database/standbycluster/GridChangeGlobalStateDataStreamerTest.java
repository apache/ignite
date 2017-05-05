package org.apache.ignite.cache.database.standbycluster;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;

/**
 *
 */
public class GridChangeGlobalStateDataStreamerTest extends GridChangeGlobalStateAbstractTest {
    /** {@inheritDoc} */
    @Override protected int backUpNodes() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected int backUpClientNodes() {
        return 0;
    }

    /**
     *
     */
    public void testDeActivateAndActivateDataStreamer() throws InterruptedException {

        Ignite ig1 = primary(0);
        Ignite ig2 = primary(1);
        Ignite ig3 = primary(2);

        Ignite ig1C = primaryClient(0);
        Ignite ig2C = primaryClient(1);
        Ignite ig3C = primaryClient(2);

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        assertTrue(ig1C.active());
        assertTrue(ig2C.active());
        assertTrue(ig3C.active());

        String cacheName = "myStreamCache";

        ig2C.getOrCreateCache(cacheName);

        try (IgniteDataStreamer<Integer, String> stmr = ig1.dataStreamer(cacheName)) {
            for (int i = 0; i < 100; i++)
                stmr.addData(i, Integer.toString(i));
        }

        ig2C.active(false);

        assertTrue(!ig1.active());
        assertTrue(!ig2.active());
        assertTrue(!ig3.active());

        assertTrue(!ig1C.active());
        assertTrue(!ig2C.active());
        assertTrue(!ig3C.active());

        boolean fail = false;

        try {
            IgniteDataStreamer<String, String> strm2 = ig2.dataStreamer(cacheName);
        }
        catch (Exception e) {
            fail = true;

            assertTrue(e.getMessage().contains("can not perform operation, because cluster inactive"));
        }

        if (!fail)
            fail("exception was not throw");

        ig3C.active(true);

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        assertTrue(ig1C.active());
        assertTrue(ig2C.active());
        assertTrue(ig3C.active());

        try (IgniteDataStreamer<Integer, String> stmr2 = ig2.dataStreamer(cacheName)) {
            for (int i = 100; i < 200; i++)
                stmr2.addData(i, Integer.toString(i));
        }

        IgniteCache<Integer, String> cache = ig3.cache(cacheName);

        for (int i = 0; i < 200; i++)
            assertEquals(String.valueOf(i), cache.get(i));
    }
}
