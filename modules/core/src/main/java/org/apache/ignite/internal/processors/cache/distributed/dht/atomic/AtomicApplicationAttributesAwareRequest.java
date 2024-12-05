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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/** Wraps atomic updates with application attributes. */
public class AtomicApplicationAttributesAwareRequest extends GridCacheIdMessage {
    /** */
    public static final short TYPE_CODE = 180;

    /** Original update message. */
    private GridNearAtomicAbstractUpdateRequest payload;

    /** Application attributes. */
    private Map<String, String> appAttrs;

    /** */
    public AtomicApplicationAttributesAwareRequest() {
    }

    /** */
    public AtomicApplicationAttributesAwareRequest(
        GridNearAtomicAbstractUpdateRequest payload,
        Map<String, String> appAttrs
    ) {
        this.payload = payload;
        this.appAttrs = appAttrs;
        cacheId = payload.cacheId();
    }

    /** @return Original update message. */
    public GridNearAtomicAbstractUpdateRequest payload() {
        return payload;
    }

    /** @return Application attributes. */
    public Map<String, String> applicationAttributes() {
        return appAttrs;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        payload.prepareMarshal(ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        payload.finishUnmarshal(ctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 4:
                if (!writer.writeMap("appAttrs", appAttrs, MessageCollectionItemType.STRING, MessageCollectionItemType.STRING))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("payload", payload))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 4:
                appAttrs = reader.readMap("appAttrs", MessageCollectionItemType.STRING, MessageCollectionItemType.STRING, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                payload = reader.readMessage("payload");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 6;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
