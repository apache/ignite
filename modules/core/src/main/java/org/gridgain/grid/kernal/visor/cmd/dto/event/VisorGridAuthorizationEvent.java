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
 * Lightweight counterpart for {@link GridAuthorizationEvent}.
 */
public class VisorGridAuthorizationEvent extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /** Requested operation. */
    private final GridSecurityPermission operation;

    /** Authenticated subject authorized to perform operation. */
    private final GridSecuritySubject subject;

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
     */
    public VisorGridAuthorizationEvent(
        int typeId,
        GridUuid id,
        String name,
        UUID nid,
        long timestamp,
        String message,
        String shortDisplay,
        GridSecurityPermission operation,
        GridSecuritySubject subject
    ) {
        super(typeId, id, name, nid, timestamp, message, shortDisplay);

        this.operation = operation;
        this.subject = subject;
    }

    /**
     * Gets requested operation.
     *
     * @return Requested operation.
     */
    public GridSecurityPermission operation() {
        return operation;
    }

    /**
     * Gets authenticated subject.
     *
     * @return Authenticated subject.
     */
    public GridSecuritySubject subject() {
        return subject;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridAuthorizationEvent.class, this);
    }
}
