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

package org.apache.ignite.internal.processors.query.schema.message;

import java.util.LinkedHashMap;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Holder for active schema change propose discovery messages.
 */
public class ActiveProposals implements Message {
    /** Active proposals. */
    @Order(0)
    LinkedHashMap<UUID, SchemaProposeDiscoveryMessage> activeProposals;

    /** */
    public ActiveProposals() {
        // No-op.
    }

    /**
     * @param activeProposals Active proposals.
     */
    public ActiveProposals(LinkedHashMap<UUID, SchemaProposeDiscoveryMessage> activeProposals) {
        this.activeProposals = activeProposals;
    }

    /**
     * @return Active proposals.
     */
    public LinkedHashMap<UUID, SchemaProposeDiscoveryMessage> activeProposals() {
        return activeProposals;
    }
}
