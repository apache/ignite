/* @java.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.plugin;

import ro.fortsoft.pf4j.*;

/**
 * Abstract base class for Visor plugin extension point.
 */
public abstract class VisorExtensionPoint implements ExtensionPoint {
    /** */
    private final VisorPluginModel model;

    /**
     * @param model Visor model.
     */
    protected VisorExtensionPoint(VisorPluginModel model) {
        this.model = model;
    }

    /**
     * @return Visor model.
     */
    public VisorPluginModel model() {
        return model;
    }

    /**
     * @return Plugin name.
     */
    public abstract String name();

    /**
     * Will be executed on Visor model changed.
     */
    public void onModelChanged() {
        // No-op.
    }

    /**
     * Will be executed on Visor events changed.
     */
    public void onEventsChanged() {
        // No-op.
    }

    /**
     * Will be executed on Visor connect to grid.
     */
    public void onConnected() {
        // No-op.
    }

    /**
     * Will be executed on Visor disconnect from grid.
     */
    public void onDisconnected() {
        // No-op.
    }
}
