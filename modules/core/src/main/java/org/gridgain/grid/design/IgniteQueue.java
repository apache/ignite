// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design;

import org.gridgain.grid.design.configuration.*;

import java.util.concurrent.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface IgniteQueue<T> extends BlockingQueue<T> {
    /**
     *
     * @return Queue configuration.
     */
    public QueueConfiguration configuration();
}
