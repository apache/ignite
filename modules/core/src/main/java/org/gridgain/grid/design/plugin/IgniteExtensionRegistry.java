/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.plugin;

/**
 * Context that allow to register extensions.
 */
public interface IgniteExtensionRegistry {
    /**
     * Register extension provided by plugin.
     *
     * @param extensionItf Extension interface.
     * @param extensionImpl Extension implementation.
     * @param <T> Extension type.
     */
    public <T> void registerExtension(Class<T> extensionItf, T extensionImpl);
}
