/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.visor.plugin.api;

import java.util.*;

/**
 * Standart Visor panel with nodes.
 *
 * This panel will be created by {@link VisorPluginComponentsFactory}.
 */
public interface VisorPluginNodesPanel extends VisorPluginComponent {
    /**
     * Add selection listener.
     *
     * @param listener Table selection listener.
     */
    public void addSelectionListener(VisorSelectionListener<UUID> listener);

    /**
     * Remove selection listener.
     *
     * @param listener Table selection listener.
     */
    public void removeSelectionListener(VisorSelectionListener<UUID> listener);

    /**
     * Get selected elements.
     *
     * @return selected elements.
     */
    public Collection<UUID> selected();
}
