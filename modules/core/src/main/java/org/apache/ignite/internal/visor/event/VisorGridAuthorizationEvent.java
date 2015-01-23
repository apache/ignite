/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.event;

import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

/**
 * Lightweight counterpart for {@link org.apache.ignite.events.IgniteAuthorizationEvent}.
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
        IgniteUuid id,
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
