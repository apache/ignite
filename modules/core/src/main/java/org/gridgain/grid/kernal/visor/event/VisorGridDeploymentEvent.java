/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.event;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Lightweight counterpart for {@link org.apache.ignite.events.GridDeploymentEvent}.
 */
public class VisorGridDeploymentEvent extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deployment alias. */
    private final String alias;

    /**
     * Create event with given parameters.
     *
     * @param typeId Event type.
     * @param id Event id.
     * @param name Event name.
     * @param nid Event node ID.
     * @param timestamp Event timestamp.
     * @param message Event message.
     * @param shortDisplay Shortened version of {@code toString()} result.
     * @param alias Deployment alias.
     */
    public VisorGridDeploymentEvent(
        int typeId,
        IgniteUuid id,
        String name,
        UUID nid,
        long timestamp,
        @Nullable String message,
        String shortDisplay,
        String alias
    ) {
        super(typeId, id, name, nid, timestamp, message, shortDisplay);

        this.alias = alias;
    }

    /**
     * @return Deployment alias.
     */
    public String alias() {
        return alias;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridDeploymentEvent.class, this);
    }
}
