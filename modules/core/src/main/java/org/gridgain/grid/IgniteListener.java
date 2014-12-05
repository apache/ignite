/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.apache.ignite.*;
import org.jetbrains.annotations.*;
import java.util.*;

/**
 * Listener for gird state change notifications. Use
 * {@link org.apache.ignite.Ignition#addListener(IgniteListener)} to register this
 * listener with grid factory.
 */
public interface IgniteListener extends EventListener {
    /**
     * Listener for grid factory state change notifications.
     *
     * @param name Grid name ({@code null} for default un-named grid).
     * @param state New state.
     */
    public void onStateChange(@Nullable String name, IgniteState state);
}
