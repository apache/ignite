/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.cache.cloner;

import org.apache.ignite.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Basic cache cloner based on utilization of {@link Cloneable} interface. If
 * a passed in object implements {@link Cloneable} then its implementation of
 * {@link Object#clone()} is used to get a copy. Otherwise, the object itself
 * will be returned without cloning.
 */
public class GridCacheBasicCloner implements GridCacheCloner {
    /** {@inheritDoc} */
    @Override public <T> T cloneValue(T val) throws IgniteCheckedException {
        return X.cloneObject(val, false, true);
    }
}
