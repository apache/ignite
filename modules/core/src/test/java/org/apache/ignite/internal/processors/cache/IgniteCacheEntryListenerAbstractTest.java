/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.gridgain.grid.cache.*;

import javax.cache.configuration.*;
import javax.cache.event.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 *
 */
public class IgniteCacheEntryListenerAbstractTest extends IgniteCacheAbstractTest {
    @Override protected int gridCount() {
        return 3;
    }

    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    @Override protected GridCacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    @Override protected GridCacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return PRIMARY;
    }

    @Override
    protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setIncludeEventTypes(IgniteEventType.EVT_CACHE_OBJECT_PUT);

        return cfg;
    }

    @Override
    protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = super.cacheConfiguration(gridName);

        //ccfg.setBackups(1);

        return ccfg;
    }

    public void testEvent() throws Exception {
        Ignite ignite = ignite(0);

        /*
        ignite.events().remoteListen(new IgniteBiPredicate<UUID, IgniteEvent>() {
            @Override public boolean apply(UUID uuid, IgniteEvent e) {
                IgniteCacheEvent evt0 = (IgniteCacheEvent)e;

                System.out.println("Event: " + uuid + " " + evt0.eventNode() + " " + e);

                return false;
            }
        }, null, IgniteEventType.EVT_CACHE_OBJECT_PUT);
        */

        IgniteCache<Integer, Integer> cache = jcache();

        final CacheEntryCreatedListener<Integer, Integer> lsnr = new CacheEntryCreatedListener<Integer, Integer>() {
            @Override public void onCreated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) throws CacheEntryListenerException {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts) {
                    System.out.println("Event: " + evt.getEventType() + " " + evt.getKey() + " " + evt.getOldValue() + " " + evt.getValue());
                }
            }
        };

        CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg = new MutableCacheEntryListenerConfiguration(
            new Factory<CacheEntryListener>() {
                @Override public CacheEntryListener create() {
                    return lsnr;
                }
            },
            null,
            true,
            false
        );

        cache.registerCacheEntryListener(lsnrCfg);

        ignite(1).cache(null).put(1, 1);
        ignite(0).cache(null).put(1, 2);

        Thread.sleep(2000);
    }
}
