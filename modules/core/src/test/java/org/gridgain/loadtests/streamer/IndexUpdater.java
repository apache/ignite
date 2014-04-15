/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.streamer;

import org.gridgain.grid.streamer.index.*;
import org.jetbrains.annotations.*;

/**
 * Streamer benchmark window index updater.
 */
class IndexUpdater implements GridStreamerIndexUpdater<Integer, Integer, Long> {
    /** {@inheritDoc} */
    @Override public Integer indexKey(Integer evt) {
        return evt;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Long onAdded(GridStreamerIndexEntry<Integer, Integer, Long> entry, Integer evt) {
        return entry.value() + 1;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Long onRemoved(GridStreamerIndexEntry<Integer, Integer, Long> entry, Integer evt) {
        return entry.value() - 1 == 0 ? null : entry.value() - 1;
    }

    /** {@inheritDoc} */
    @Override public Long initialValue(Integer evt, Integer key) {
        return 1L;
    }
}
