/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.events;

import org.gridgain.grid.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Cache query read event.
 */
public class GridCacheFieldsQueryReadEvent extends GridCacheQueryEvent {
    /** Result row. */
    @GridToStringInclude
    private final List<?> row;

    /**
     * @param node Node where event was fired.
     * @param msg Event message.
     * @param type Event type.
     * @param cacheName Cache name.
     * @param clause Clause.
     * @param args Query arguments.
     * @param subjId Security subject ID.
     * @param row Result row.
     */
    public GridCacheFieldsQueryReadEvent(GridNode node, String msg, int type, @Nullable String cacheName,
        String clause, @Nullable Object[] args, @Nullable UUID subjId, List<?> row) {
        super(node, msg, type, cacheName, null, clause, null, args, subjId);

        assert row != null;

        this.row = row;
    }

    /**
     * Gets read results set row.
     *
     * @return Result row.
     */
    public List<?> row() {
        return row;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheFieldsQueryReadEvent.class, this, super.toString());
    }
}
