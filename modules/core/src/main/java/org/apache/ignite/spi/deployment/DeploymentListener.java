/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment;

/**
 * Listener for deployment events. It is used by grid implementation
 * to properly create or release resources associated with any deployment.
 */
public interface DeploymentListener {
    /**
     * Called when a deployment has been unregistered..
     *
     * @param ldr Registered class loader.
     */
    public void onUnregistered(ClassLoader ldr);
}
