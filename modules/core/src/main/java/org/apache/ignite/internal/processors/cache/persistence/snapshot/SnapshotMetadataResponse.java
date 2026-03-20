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

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MarshallableMessage;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/** */
public class SnapshotMetadataResponse implements MarshallableMessage {
    /** */
    @Order(0)
    byte[] metadataBytes;

    /** */
    private List<SnapshotMetadata> metadata;

    /** Default constructor for {@link MessageFactory}. */
    public SnapshotMetadataResponse() {
        // No-op.
    }

    /** */
    public SnapshotMetadataResponse(List<SnapshotMetadata> metadata) {
        this.metadata = metadata;
    }

    /** */
    public List<SnapshotMetadata> metadata() {
        return metadata;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        metadataBytes = U.marshal(marsh, metadata);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        if (metadataBytes != null)
            metadata = U.unmarshal(marsh, metadataBytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 526;
    }
}
