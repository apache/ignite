/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.visor.plugin;

import ro.fortsoft.pf4j.*;

/**
 * Base class for Visor plugins.
 */
public abstract class VisorPlugin extends Plugin {
    /** {@inheritDoc} */
    public VisorPlugin(PluginWrapper wrapper) {
        super(wrapper);
    }

    /**
     * @return Plugin name.
     */
    public abstract String name();

    /** {@inheritDoc} */
    @Override public void start() throws PluginException {
        log.info("Plugin Started: " + name());
    }

    /** {@inheritDoc} */
    @Override public void stop() throws PluginException {
        log.info("Plugin stopped: " + name());
    }
}
