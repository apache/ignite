/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

/**
 * Internal wrapper interface for custom resource injection logic.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridInternalWrapper<T> {
    /**
     * Get user object where resources must be injected.
     *
     * @return User object.
     */
    public T userObject();
}
