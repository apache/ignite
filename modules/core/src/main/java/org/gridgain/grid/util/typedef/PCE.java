/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.typedef;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.lang.*;

/**
 * Defines {@code alias} for <tt>GridPredicate&lt;GridCacheEntry&lt;K, V&gt;&gt;</tt> by extending
 * {@link org.gridgain.grid.lang.IgnitePredicate}. Since Java doesn't provide type aliases (like Scala, for example) we resort
 * to these types of measures. This is intended to provide for more concise code without sacrificing
 * readability. For more information see {@link org.gridgain.grid.lang.IgnitePredicate} and {@link GridCacheEntry}.
 * @see org.gridgain.grid.lang.IgnitePredicate
 * @see GridFunc
 * @see GridCacheEntry
 */
public interface PCE<K, V> extends IgnitePredicate<GridCacheEntry<K, V>> { /* No-op. */ }
