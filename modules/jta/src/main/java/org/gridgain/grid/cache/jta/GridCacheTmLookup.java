/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.jta;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.jta.jndi.*;
import org.gridgain.grid.cache.jta.reflect.*;
import org.jetbrains.annotations.*;

import javax.transaction.*;

/**
 * Allows grid to use different transactional systems. Implement this interface
 * to look up native transaction manager within your environment. Transaction
 * manager lookup is configured via {@link GridCacheConfiguration#getTransactionManagerLookupClassName()}
 * method.
 * <p>
 * The following implementations are provided out of the box:
 * <ul>
 * <li>
 *  {@link GridCacheJndiTmLookup} utilizes a configured JNDI name to look up a transaction manager.
 * </li>
 * <li>
 *  {@link GridCacheReflectionTmLookup} uses reflection to call a method on a given class
 *  to get to transaction manager.
 * </li>
 * </ul>
 */
public interface GridCacheTmLookup {
    /**
     * Gets Transaction Manager (TM).
     *
     * @return TM or {@code null} if TM cannot be looked up. 
     * @throws GridException In case of error.
     */
    @Nullable public TransactionManager getTm() throws GridException;
}
