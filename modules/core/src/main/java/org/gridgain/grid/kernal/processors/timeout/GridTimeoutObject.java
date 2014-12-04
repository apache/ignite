/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.timeout;

import org.gridgain.grid.*;

/**
 * All objects that can timeout should implement this interface.
 */
public interface GridTimeoutObject {
    /**
     * @return ID of the object.
     */
    public IgniteUuid timeoutId();

    /**
     * @return End time.
     */
    public long endTime();

    /**
     * Timeout callback.
     */
    public void onTimeout();
}
