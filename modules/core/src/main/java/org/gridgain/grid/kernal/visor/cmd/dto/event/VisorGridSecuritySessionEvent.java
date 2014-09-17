/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.event;

import org.gridgain.grid.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Lightweight counterpart for {@link VisorGridSecuritySessionEvent}.
 */
public class VisorGridSecuritySessionEvent extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /**  Subject type. */
    private final GridSecuritySubjectType subjType;

    /** Subject ID. */
    private final UUID subjId;

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
     * @param subjType Subject type.
     * @param subjId Subject ID.
     */
    public VisorGridSecuritySessionEvent(
        int typeId,
        GridUuid id,
        String name,
        UUID nid,
        long timestamp,
        String message,
        String shortDisplay,
        GridSecuritySubjectType subjType,
        UUID subjId
    ) {
        super(typeId, id, name, nid, timestamp, message, shortDisplay);

        this.subjType = subjType;
        this.subjId = subjId;
    }

    /**
     * Gets subject ID that triggered the event.
     *
     * @return Subject ID that triggered the event.
     */
    public UUID subjId() {
        return subjId;
    }

    /**
     * Gets subject type that triggered the event.
     *
     * @return Subject type that triggered the event.
     */
    public GridSecuritySubjectType subjType() {
        return subjType;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridSecuritySessionEvent.class, this);
    }
}
