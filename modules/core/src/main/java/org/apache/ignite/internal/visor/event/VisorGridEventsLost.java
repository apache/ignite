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

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Special event for events lost situations.
 */
public class VisorGridEventsLost extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Create event with given parameters.
     *
     * @param nid Node where events were lost.
     */
    public VisorGridEventsLost(UUID nid) {
        super(0, IgniteUuid.randomUuid(), "EVT_VISOR_EVENTS_LOST", nid, U.currentTimeMillis(),
            "Some Visor events were lost and Visor may show inconsistent results. " +
            "Configure your grid to disable not important events.", "");
    }
}