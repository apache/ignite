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

package org.apache.ignite.spi.discovery.tcp.messages;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Message for {@link IgniteProductVersion}.*/
public class IgniteProductVersionMessage implements Message {
    /** Size of the {@link #revHash }*/
    public static final int REV_HASH_SIZE = 20;

    /** Major version number. */
    @Order(0)
    public byte major;

    /** Minor version number. */
    @Order(1)
    public byte minor;

    /** Maintenance version number. */
    @Order(2)
    public byte maintenance;

    /** Stage of development. */
    @Order(3)
    public String stage;

    /** Revision timestamp. */
    @Order(4)
    public long revTs;

    /** Revision hash. */
    @Order(5)
    public byte[] revHash;

    /** Constructor for {@link DiscoveryMessageFactory}. */
    public IgniteProductVersionMessage() {
        // No-op.
    }

    /**
     * @param major Major version.
     * @param minor Minor version.
     * @param maintenance Maintenance.
     * @param stage Stage.
     * @param revTs Revision timestamp.
     * @param revHash Revision hash.
     */
    public IgniteProductVersionMessage(byte major, byte minor, byte maintenance, String stage, long revTs, byte[] revHash) {
        this.major = major;
        this.minor = minor;
        this.maintenance = maintenance;
        this.stage = stage;
        this.revTs = revTs;
        this.revHash = revHash != null ? revHash : new byte[REV_HASH_SIZE];
    }

    /** @param ver Product version. */
    public IgniteProductVersionMessage(IgniteProductVersion ver) {
        this(
            ver.major(),
            ver.minor(),
            ver.maintenance(),
            ver.stage(),
            ver.revisionTimestamp(),
            ver.revisionHash()
        );
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -109;
    }
}
