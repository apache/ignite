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
 * Cache deep cloner that creates a copy of an object using deep reflection.
 * <p>
 * If {@link #isHonorCloneable()} is set to {@code true}, then deep cloner will
 * first check if the passed in object implements {@link Cloneable} interface and,
 * if it does, will delegate to {@code clone()} method. It is advisable for
 * instances that need to be cloned to implement {@link Cloneable}, as cloning
 * this way will generally be faster than reflection-based cloning.
 * <p>
 * This implementation will first check if the object to clone has an empty
 * constructor. If it does, then it will be instantiated using such constructor.
 * Otherwise an empty constructor will be fetched from JDK and used instead.
 * Note that this behavior may not work on some JDKs in which case
 * {@link #cloneValue(Object)} method will result in {@link IgniteCheckedException}
 * being thrown.
 */
public class GridCacheDeepCloner implements GridCacheCloner {
    /** */
    private boolean honorCloneable = true;

    /**
     * Creates deep cloner with {@link #isHonorCloneable()} flag set to {@code true}.
     */
    public GridCacheDeepCloner() {
        // No-op.
    }

    /**
     * Creates a new instance of deep cloner with specified flag to honor
     * {@link Cloneable} interface or not.
     *
     * @param honorCloneable Flag indicating whether {@link Cloneable}
     *      interface should be honored or not when cloning.
     */
    public GridCacheDeepCloner(boolean honorCloneable) {
        this.honorCloneable = honorCloneable;
    }

    /**
     * Gets flag indicating if {@link Cloneable} interface should be honored
     * when cloning, or if reflection-based deep cloning should always be performed.
     *
     * @return Flag indicating if {@link Cloneable} interface should be honored
     *      when cloning
     */
    public boolean isHonorCloneable() {
        return honorCloneable;
    }

    /**
     * Sets flag indicating if {@link Cloneable} interface should be honored
     * when cloning, or if reflection-based deep cloning should always be performed.
     *
     * @param honorCloneable Flag indicating whether {@link Cloneable} interface
     *      should be honored or not when cloning.
     */
    public void setHonorCloneable(boolean honorCloneable) {
        this.honorCloneable = honorCloneable;
    }

    /** {@inheritDoc} */
    @Override public <T> T cloneValue(T val) throws IgniteCheckedException {
        return X.cloneObject(val, true, honorCloneable);
    }
}
