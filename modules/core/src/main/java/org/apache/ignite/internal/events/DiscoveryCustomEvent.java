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

package org.apache.ignite.internal.events;

import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Custom event.
 */
public class DiscoveryCustomEvent extends DiscoveryEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Built-in event type: custom event sent.
     * <br>
     * Generated when someone invoke {@link GridDiscoveryManager#sendCustomEvent(DiscoveryCustomMessage)}.
     * <p>
     *
     * @see DiscoveryCustomEvent
     */
    public static final int EVT_DISCOVERY_CUSTOM_EVT = 18;

    /** */
    private DiscoveryCustomMessage customMsg;

    /** Affinity topology version. */
    private AffinityTopologyVersion affTopVer;

    /**
     * Default constructor.
     */
    public DiscoveryCustomEvent() {
        type(EVT_DISCOVERY_CUSTOM_EVT);
    }

    /**
     * @return Data.
     */
    public DiscoveryCustomMessage customMessage() {
        return customMsg;
    }

    /**
     * @param customMsg New customMessage.
     */
    public void customMessage(DiscoveryCustomMessage customMsg) {
        this.customMsg = customMsg;
    }

    /**
     * @return Affinity topology version.
     */
    public AffinityTopologyVersion affinityTopologyVersion() {
        return affTopVer;
    }

    /**
     * @param affTopVer Affinity topology version.
     */
    public void affinityTopologyVersion(AffinityTopologyVersion affTopVer) {
        this.affTopVer = affTopVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiscoveryCustomEvent.class, this, super.toString());
    }
}