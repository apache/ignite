/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.examples.datagrid.store.dummy;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.transactions.*;
import org.gridgain.examples.datagrid.store.*;
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
    @Override public Person load(@Nullable IgniteTx tx, Long key) throws IgniteCheckedException {
        System.out.println(">>> Store load [key=" + key + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        return dummyDB.get(key);
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable IgniteTx tx, Long key, Person val) throws IgniteCheckedException {
        System.out.println(">>> Store put [key=" + key + ", val=" + val + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        dummyDB.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable IgniteTx tx, Long key) throws IgniteCheckedException {
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
