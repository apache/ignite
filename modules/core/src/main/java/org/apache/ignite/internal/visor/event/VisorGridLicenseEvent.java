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
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Lightweight counterpart for {@link org.apache.ignite.events.LicenseEvent}.
 */
public class VisorGridLicenseEvent extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /** License ID. */
    private final UUID licenseId;

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
     * @param licenseId License ID.
     */
    public VisorGridLicenseEvent(
        int typeId,
        IgniteUuid id,
        String name,
        UUID nid,
        long timestamp,
        @Nullable String message,
        String shortDisplay,
        UUID licenseId
    ) {
        super(typeId, id, name, nid, timestamp, message, shortDisplay);

        this.licenseId = licenseId;
    }

    /**
     * @return License ID.
     */
    public UUID licenseId() {
        return licenseId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridLicenseEvent.class, this);
    }
}
