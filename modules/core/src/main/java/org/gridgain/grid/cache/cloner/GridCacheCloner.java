/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.cloner;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;
import org.jetbrains.annotations.*;

/**
 * Cache cloner which clones cached values before returning them from cache.
 * It will only be used if {@link GridCacheFlag#CLONE} flag is set
 * on projection which the user is working with (this flag is disabled
 * by default).
 * <p>
 * This behaviour is useful, as a an example, when we need to get some object
 * from cache, change some of its properties and put it back into cache.
 * In such a scenario it would be wrong to change properties of cached value
 * itself without creating a copy first, since it would break cache integrity,
 * and will affect the cached values returned to other threads even before
 * the transaction commits.
 * <p>
 * Cache cloner can be set in cache configuration via {@link GridCacheConfiguration#getCloner()}
 * method. By default, cache uses {@link GridCacheBasicCloner} implementation
 * which will clone only objects implementing {@link Cloneable} interface. You
 * can also configure cache to use {@link GridCacheDeepCloner} which will perform
 * deep-cloning of all objects returned from cache, regardless of the
 * {@link Cloneable} interface. If none of the above cloners fit your logic, you
 * can also provide your own implementation of this interface.
 *
 * @see GridCacheBasicCloner
 * @see GridCacheDeepCloner
 * @see GridCacheConfiguration#getCloner()
 * @see GridCacheConfiguration#setCloner(GridCacheCloner)
 *
 */
public interface GridCacheCloner {
    /**
     * @param val Object to make a clone for.
     * @throws IgniteCheckedException If failed to clone given object.
     * @return Clone for given object.
     */
    @Nullable public <T> T cloneValue(T val) throws IgniteCheckedException;
}
