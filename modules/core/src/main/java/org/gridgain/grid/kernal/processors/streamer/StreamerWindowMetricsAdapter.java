/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Streamer window metrics adapter.
 */
public class StreamerWindowMetricsAdapter implements StreamerWindowMetrics {
    /** Window name. */
    private String name;

    /** Window size. */
    private int size;

    /** Window eviction queue size. */
    private int evictionQueueSize;

    /**
     * @param m Metrics to copy.
     */
    public StreamerWindowMetricsAdapter(StreamerWindowMetrics m) {
        // Preserve alphabetic order for maintenance.
        evictionQueueSize = m.evictionQueueSize();
        name = m.name();
        size = m.size();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public int evictionQueueSize() {
        return evictionQueueSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StreamerWindowMetricsAdapter.class, this);
    }
}
