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

package org.apache.ignite.cdc;

import java.io.Serializable;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Entry event order.
 * Two concurrent updates of the same entry can be ordered based on {@link ChangeEventOrder} comparsion.
 * Greater value means that event occurs later.
 */
@IgniteExperimental
public interface ChangeEventOrder extends Comparable<ChangeEventOrder>, Serializable {
    /** @return topVer Topology version plus number of seconds from the start time of the first grid node. */
    public int topologyVersion();

    /** @return Node order on which this version was assigned. */
    public int nodeOrder();

    /** @return Data center id. */
    public byte dataCenterId();

    /** @return order Version order. */
    public long order();

    /** @return Replication version. */
    public ChangeEventOrder otherDcOrder();
}
