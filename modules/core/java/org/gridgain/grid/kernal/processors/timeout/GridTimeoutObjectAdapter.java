/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.timeout;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Default implementation for {@link GridTimeoutObject}.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridTimeoutObjectAdapter implements GridTimeoutObject {
    /** Timeout ID. */
    private final GridUuid id;

    /** End time. */
    private final long endTime;

    /**
     * @param timeout Timeout for this object.
     */
    protected GridTimeoutObjectAdapter(long timeout) {
        this(GridUuid.randomUuid(), timeout);
    }

    /**
     * @param id Timeout ID.
     * @param timeout Timeout for this object.
     */
    protected GridTimeoutObjectAdapter(GridUuid id, long timeout) {
        this.id = id;

        long endTime = timeout >= 0 ? U.currentTimeMillis() + timeout : Long.MAX_VALUE;

        this.endTime = endTime >= 0 ? endTime : Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public GridUuid timeoutId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return endTime;
    }
}
