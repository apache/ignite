package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.GridKernalContext;

/**
 * Context to get value of cache object.
 */
public interface CacheObjectValueContext {
    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext();

    /**
     * @return Copy on get flag.
     */
    public boolean copyOnGet();

    /**
     * @return {@code True} if should store unmarshalled value in cache.
     */
    public boolean storeValue();

    /**
     * @return {@code True} if deployment info should be associated with the objects of this cache.
     */
    public boolean addDeploymentInfo();

    /**
     * @return Binary enabled flag.
     */
    public boolean binaryEnabled();
}
