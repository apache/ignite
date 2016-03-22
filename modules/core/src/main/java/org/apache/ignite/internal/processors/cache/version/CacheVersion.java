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

package org.apache.ignite.internal.processors.cache.version;

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

import java.io.Externalizable;

/**
 *
 */
public interface CacheVersion extends Comparable<CacheVersion>, Message, Externalizable {
    /**
     * @return Topology version plus number of seconds from the start time of the first grid node..
     */
    public int topologyVersion();

    /**
     * @return Adjusted time.
     */
    public long globalTime();

    /**
     * @return Version order.
     */
    public long order();

    /**
     * @return Node order on which this version was assigned.
     */
    public int nodeOrder();

    /**
     * @return DR mask.
     */
    public byte dataCenterId();

    /**
     * @return {@code True} if has conflict version.
     */
    public boolean hasConflictVersion();

    /**
     * @return Conflict version.
     */
    public CacheVersion conflictVersion();

    /**
     * @return Version represented as {@code GridUuid}
     */
    public IgniteUuid asGridUuid();

    /**
     * @return Combined integer for node order and DR ID.
     */
    public int nodeOrderAndDrIdRaw();
}
