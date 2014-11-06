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
import org.gridgain.grid.security.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Lightweight counterpart for {@link GridAuthenticationEvent}.
 */
public class VisorGridAuthenticationEvent extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /**  Subject type. */
    private final GridSecuritySubjectType subjType;

    /** Subject ID. */
    private final UUID subjId;

    /** Login. */
    private final Object login;

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
     * @param login Login object.
     */
    public VisorGridAuthenticationEvent(
        int typeId,
        GridUuid id,
        String name,
        UUID nid,
        long timestamp,
        String message,
        String shortDisplay,
        GridSecuritySubjectType subjType,
        UUID subjId,
        Object login
    ) {
        super(typeId, id, name, nid, timestamp, message, shortDisplay);

        this.subjType = subjType;
        this.subjId = subjId;
        this.login = login;
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
     * Gets login that triggered event.
     *
     * @return Login object.
     */
    public Object login() {
        return login;
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
        return S.toString(VisorGridAuthenticationEvent.class, this);
    }
}
