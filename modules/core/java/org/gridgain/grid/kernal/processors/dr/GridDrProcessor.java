// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr;

import org.gridgain.grid.dr.*;
import org.gridgain.grid.kernal.processors.*;
import org.jetbrains.annotations.*;

/**
 * Data center replication processor.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridDrProcessor<K, V> extends GridProcessor {
    /**
     * @return DR public view.
     */
    @Nullable public GridDr dr();
}
