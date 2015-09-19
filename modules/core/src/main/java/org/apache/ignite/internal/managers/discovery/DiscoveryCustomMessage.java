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

package org.apache.ignite.internal.managers.discovery;

import java.io.Serializable;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface DiscoveryCustomMessage extends Serializable {
    /**
     * @return Unique custom message ID.
     */
    public IgniteUuid id();

    /**
     * Whether or not minor version of topology should be increased on message receive.
     *
     * @return {@code true} if minor topology version should be increased.
     * @see AffinityTopologyVersion#minorTopVer
     */
    public boolean incrementMinorTopologyVersion();

    /**
     * Called when custom message has been handled by all nodes.
     *
     * @return Ack message or {@code null} if ack is not required.
     */
    @Nullable public DiscoveryCustomMessage ackMessage();

    /**
     * @return {@code true} if message can be modified during listener notification. Changes will be send to next nodes.
     */
    public boolean isMutable();
}