// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.store.dummy;

import org.gridgain.examples.datagrid.store.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Dummy cache store implementation.
 *
 * @author @java.author
 * @version @java.version
 */
public class CacheDummyPersonStore extends GridCacheStoreAdapter<Long, Person> {
    /** Dummy database. */
    private Map<Long, Person> dummyDB = new HashMap<>();

    /** {@inheritDoc} */
    @Override public Person load(@Nullable GridCacheTx tx, Long key) throws GridException {
        System.out.println("Store load [key=" + key + ", tx=" + tx + ']');

        return dummyDB.get(key);
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx tx, Long key, Person val) throws GridException {
        System.out.println("Store put [key=" + key + ", val=" + val + ", tx=" + tx + ']');

        dummyDB.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable GridCacheTx tx, Long key) throws GridException {
        System.out.println("Store remove [key=" + key + ", tx=" + tx + ']');

        dummyDB.remove(key);
    }
}
