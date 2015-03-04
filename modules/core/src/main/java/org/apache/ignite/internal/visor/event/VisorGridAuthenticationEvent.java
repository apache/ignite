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

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.security.*;

import java.util.*;

/**
 * Lightweight counterpart for AuthenticationEvent.
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
     * @param ts Event timestamp.
     * @param msg Event message.
     * @param shortDisplay Shortened version of {@code toString()} result.
     * @param subjType Subject type.
     * @param subjId Subject ID.
     * @param login Login object.
     */
    public VisorGridAuthenticationEvent(
        int typeId,
        IgniteUuid id,
        String name,
        UUID nid,
        long ts,
        String msg,
        String shortDisplay,
        GridSecuritySubjectType subjType,
        UUID subjId,
        Object login
    ) {
        super(typeId, id, name, nid, ts, msg, shortDisplay);

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
