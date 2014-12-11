/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.store.dummy;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.examples.datagrid.store.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Dummy cache store implementation.
 */
public class CacheDummyPersonStore extends GridCacheStoreAdapter<Long, Person> {
    /** Auto-inject grid instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Auto-inject cache name. */
    @GridCacheName
    private String cacheName;

    /** Dummy database. */
    private Map<Long, Person> dummyDB = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public Person load(@Nullable GridCacheTx tx, Long key) throws IgniteCheckedException {
        System.out.println(">>> Store load [key=" + key + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        return dummyDB.get(key);
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx tx, Long key, Person val) throws IgniteCheckedException {
        System.out.println(">>> Store put [key=" + key + ", val=" + val + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        dummyDB.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable GridCacheTx tx, Long key) throws IgniteCheckedException {
        System.out.println(">>> Store remove [key=" + key + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        dummyDB.remove(key);
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<Long, Person> clo, Object... args) throws IgniteCheckedException {
        int cnt = (Integer)args[0];

        System.out.println(">>> Store loadCache for entry count: " + cnt);

        GridCache<Long, Person> cache = ignite.cache(cacheName);

        for (int i = 0; i < cnt; i++) {
            // Generate dummy person on the fly.
            Person p = new Person(i, "first-" + i, "last-" + 1);

            // GridGain will automatically discard entries that don't belong on this node,
            // but we check if local node is primary or backup anyway just to demonstrate that we can.
            // Ideally, partition ID of a key would be stored  in the database and only keys
            // for partitions that belong on this node would be loaded from database.
            if (cache.affinity().isPrimaryOrBackup(ignite.cluster().localNode(), p.getId())) {
                // Update dummy database.
                // In real life data would be loaded from database.
                dummyDB.put(p.getId(), p);

                // Pass data to cache.
                clo.apply(p.getId(), p);
            }
        }
    }
}
