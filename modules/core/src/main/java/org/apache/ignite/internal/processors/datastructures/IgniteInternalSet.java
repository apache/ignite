package org.apache.ignite.internal.processors.datastructures;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Internal service methods used by proxy.
 */
public interface IgniteInternalSet<T> extends IgniteSet<T> {
    /**
     * @return Set ID.
     */
    public IgniteUuid id();

    /**
     * @return {@code True} if set header found in cache.
     * @throws IgniteCheckedException If failed.
     */
    public boolean checkHeader() throws IgniteCheckedException;

    /**
     * @param rmvd {@code True} to set state as removed.
     */
    public void removed(boolean rmvd);
}
