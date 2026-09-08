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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/**
 * Single-node result of the snapshot deletion distributed process.
 *
 * @see SnapshotDeleteProcess
 */
public class SnapshotDeleteResponse implements Message {
    /** The result. If {@code -1}, if node isn't a server node. {@code 0} if the snapshot was completely
     * removed on the node. {@code 1}, if snapshot was removed on the node not completely. */
    @Order(0)
    byte deleted;

    /** Default constructor for {@link MessageFactory}. */
    public SnapshotDeleteResponse() {
        // No-op.
    }

    /**
     * @param deleted If {@code -1}, if node isn't a server node. {@code 1} if the snapshot was completely
     * removed on the node. {@code 0}, if snapshot was removed on the node not completely.
     */
    SnapshotDeleteResponse(int deleted) {
        assert deleted >= -1 && deleted < 2;

        this.deleted = (byte)deleted;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotDeleteResponse.class, this);
    }
}
