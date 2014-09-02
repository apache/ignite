/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.event;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Lightweight counterpart for {@link GridDiscoveryEvent}.
 */
public class VisorGridDiscoveryEvent extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node that caused this event to be generated. */
    private final UUID evtNodeId;

    /** Node address that caused this event to be generated. */
    private final String addr;

    /** If node that caused this event is daemon. */
    private final boolean isDaemon;

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
     * @param evtNodeId Event node id.
     * @param addr Event node address.
     * @param isDaemon If event node is daemon on not.
     */
    public VisorGridDiscoveryEvent(
        int typeId,
        GridUuid id,
        String name,
        UUID nid,
        long timestamp,
        @Nullable String message,
        String shortDisplay,
        UUID evtNodeId,
        String addr,
        boolean isDaemon
    ) {
        super(typeId, id, name, nid, timestamp, message, shortDisplay);

        this.evtNodeId = evtNodeId;
        this.addr = addr;
        this.isDaemon = isDaemon;
    }

    /**
     * @return Deployment alias.
     */
    public UUID evtNodeId() {
        return evtNodeId;
    }

    /**
     * @return Node address that caused this event to be generated.
     */
    public String address() {
        return addr;
    }

    /**
     * @return If node that caused this event is daemon.
     */
    public boolean isDaemon() {
        return isDaemon;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridDiscoveryEvent.class, this);
    }
}
