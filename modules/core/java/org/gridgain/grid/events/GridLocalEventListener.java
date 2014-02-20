// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.events;

import java.io.*;
import java.util.*;

/**
 * Listener for asynchronous local node grid events. You can subscribe for local node grid
 * event notifications via {@link GridEvents#addLocalListener(GridLocalEventListener, int...)}.
 * <p>
 * Use {@link GridEvents#addLocalListener(GridLocalEventListener, int...)} to register
 * this listener with grid.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridLocalEventListener extends EventListener, Serializable {
    /**
     * Local event callback.
     *
     * @param evt local grid event.
     */
    public void onEvent(GridEvent evt);
}
