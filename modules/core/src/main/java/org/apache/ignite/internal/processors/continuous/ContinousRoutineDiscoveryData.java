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

package org.apache.ignite.internal.processors.continuous;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Continous routine Discovery data. */
public final class ContinousRoutineDiscoveryData implements Message {
    /** Node ID.  */
    @Order(0)
    UUID nodeId;

    /** Items. */
    @GridToStringInclude
    @Order(1)
    Collection<ContinousRoutineDiscoveryDataItem> items;

    /** */
    @Order(2)
    Map<UUID, Map<UUID, ContinousRoutineLocalInfo>> clientInfos;

    /** Empty constructor for serialization purposes. */
    public ContinousRoutineDiscoveryData() {
        // No-op.
    }

    /**
     * @param nodeId Node ID.
     * @param clientInfos Client information.
     */
    ContinousRoutineDiscoveryData(UUID nodeId, Map<UUID, Map<UUID, ContinousRoutineLocalInfo>> clientInfos) {
        assert nodeId != null;

        this.nodeId = nodeId;

        this.clientInfos = clientInfos;

        items = new ArrayList<>();
    }

    /** @param item Item. */
    public void addItem(ContinousRoutineDiscoveryDataItem item) {
        items.add(item);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ContinousRoutineDiscoveryData.class, this);
    }
}
