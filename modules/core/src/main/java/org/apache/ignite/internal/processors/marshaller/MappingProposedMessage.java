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

package org.apache.ignite.internal.processors.marshaller;

import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

class MappingProposedMessage implements DiscoveryCustomMessage {

    private static final long serialVersionUID = 0L;

    private final IgniteUuid id = IgniteUuid.randomUuid();

    private final UUID origNodeId;

    private final MarshallerMappingItem mappingItem;

    private boolean inConflict;

    private String conflictingClassName;

    public MappingProposedMessage(MarshallerMappingItem mappingItem, UUID origNodeId) {
        this.mappingItem = mappingItem;
        this.origNodeId = origNodeId;
    }

    @Override
    public IgniteUuid id() {
        return id;
    }

    @Nullable
    @Override
    public DiscoveryCustomMessage ackMessage() {
        if (!inConflict) {
            return new MappingAcceptedMessage(mappingItem);
        } else
            return new MappingRejectedMessage(mappingItem, conflictingClassName, origNodeId);
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    public MarshallerMappingItem getMappingItem() {
        return mappingItem;
    }

    public boolean isInConflict() {
        return inConflict;
    }

    public void setInConflict(boolean inConflict) {
        this.inConflict = inConflict;
    }

    public void setConflictingClassName(String conflictingClassName) {
        this.conflictingClassName = conflictingClassName;
    }
}
