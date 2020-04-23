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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Affinity topology version for thin client.
 */
public class ClientAffinityTopologyVersion {
    /** Affinity topology version. */
    private final AffinityTopologyVersion version;

    /** Flag that shows whether version has changed since the last check. */
    private final boolean changed;

    /**
     * Constructor.
     *
     * @param version Affinity topology version.
     * @param changed Flag that shows whether version has changed since the last check.
     */
    public ClientAffinityTopologyVersion(AffinityTopologyVersion version, boolean changed) {
        this.version = version;
        this.changed = changed;
    }

    /**
     * @return {@code True} if version has changed since the last check.
     */
    public boolean isChanged() {
        return changed;
    }

    /**
     * @return Affinity topology version.
     */
    public AffinityTopologyVersion getVersion() {
        return version;
    }

    /**
     * Write version using binary writer.
     * @param writer Writer.
     */
    public void write(BinaryRawWriter writer) {
        AffinityTopologyVersion affinityVer0 = version;

        writer.writeLong(affinityVer0.topologyVersion());
        writer.writeInt(affinityVer0.minorTopologyVersion());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientAffinityTopologyVersion.class, this);
    }
}
