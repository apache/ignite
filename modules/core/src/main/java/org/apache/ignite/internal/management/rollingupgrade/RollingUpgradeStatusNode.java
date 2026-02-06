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

package org.apache.ignite.internal.management.rollingupgrade;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.lang.IgniteProductVersion;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;

/** Node status information for rolling upgrade. */
public class RollingUpgradeStatusNode extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(value = 0)
    UUID uuid;

    /** */
    @Order(value = 1)
    Object consistentId;

    /** */
    @Order(value = 2)
    Collection<String> addresses;

    /** */
    @Order(value = 3)
    IgniteProductVersion ver;

    /** */
    @Order(value = 4)
    long order;

    /** */
    @Order(value = 5)
    boolean client;

    /** */
    public RollingUpgradeStatusNode() {
        // No-op.
    }

    /** */
    public RollingUpgradeStatusNode(ClusterNode node) {
        ver = IgniteProductVersion.fromString(node.attribute(ATTR_BUILD_VER));
        uuid = node.id();
        consistentId = node.consistentId();
        addresses = node.addresses();
        order = node.order();
        client = node.isClient();
    }

    /** */
    public IgniteProductVersion version() {
        return ver;
    }

    /** */
    public Collection<String> addresses() {
        return addresses;
    }

    /** */
    public UUID uuid() {
        return uuid;
    }

    /** */
    public Object consistentId() {
        return consistentId;
    }

    /** */
    public long order() {
        return order;
    }

    /** */
    public boolean client() {
        return client;
    }
}
