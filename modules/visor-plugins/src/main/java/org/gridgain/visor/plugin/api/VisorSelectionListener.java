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
 * The listener interface for receiving "selection" events from standard Visor panels.
 */
public interface VisorSelectionListener<T> {
    /**
     * On selection changed.
     *
     * @param selected Selected elements. If nothing selected return all.
     */
    public void onChange(Collection<T> selected);
}
