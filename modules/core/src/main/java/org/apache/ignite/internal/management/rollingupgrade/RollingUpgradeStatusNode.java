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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;

/** Node status information for rolling upgrade. */
public class RollingUpgradeStatusNode extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    UUID uuid;

    /** */
    Object consistentId;

    /** */
    Collection<String> addresses;

    /** */
    IgniteProductVersion ver;

    /** */
    long order;

    /** */
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

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(ver);
        U.writeUuid(out, uuid);
        out.writeObject(consistentId);
        U.writeCollection(out, addresses);
        out.writeLong(order);
        out.writeBoolean(client);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        ver = (IgniteProductVersion)in.readObject();
        uuid = U.readUuid(in);
        consistentId = in.readObject();
        addresses = U.readCollection(in);
        order = in.readLong();
        client = in.readBoolean();
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
