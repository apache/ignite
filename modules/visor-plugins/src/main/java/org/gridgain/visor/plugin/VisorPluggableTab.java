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

import org.gridgain.visor.plugin.api.*;

import javax.swing.*;

/**
 * Abstract base class for Visor pluggable tabs.
 */
public abstract class VisorPluggableTab extends VisorExtensionPoint {
    /**
     * @param model Visor model.
     */
    protected VisorPluggableTab(VisorPluginModel model) {
        super(model);
    }

    /**
     * Tab and menu icon 16x16 px.
     *
     * @return Plugin icon.
     */
    public abstract ImageIcon icon();

    /**
     * Tab tooltip.
     *
     * @return Tooltip.
     */
    public abstract String tooltip();

    /**
     * Construct content of pluggable tab.
     *
     * @param componentsFactory Factory for creating ready UI blocks, like nodes panel and charts.
     * @param dfltTabBtn Panel with standart tab buttons.
     *
     * @return Pluggable tab content.
     */
    public abstract JPanel createPanel(VisorPluginComponentsFactory componentsFactory, JPanel dfltTabBtn);

    /**
     * Will be executed on tab close.
     */
    public void onClosed() {
        // No-op.
    }
}
