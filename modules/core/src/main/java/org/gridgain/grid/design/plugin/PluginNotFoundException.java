// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.plugin;

import org.gridgain.grid.design.*;

/**
 * Exception thrown if plugin is not found.
 *
 * @author @java.author
 * @version @java.version
 */
public class PluginNotFoundException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Plugin name. */
    private String name;

    /**
     * @param name Plugin name.
     */
    public PluginNotFoundException(String name) {
        super("Ignite plugin not found: " + name);
    }
}
