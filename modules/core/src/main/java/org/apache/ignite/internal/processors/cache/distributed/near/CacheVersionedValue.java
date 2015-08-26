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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;

/**
 * Cache object and version.
 */
public class CacheVersionedValue implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value. */
    @GridToStringInclude
    private CacheObject val;

    /** Cache version. */
    @GridToStringInclude
    private GridCacheVersion ver;

    /** */
    public CacheVersionedValue() {
        // No-op.
    }

    /**
     * @param val Cache value.
     * @param ver Cache version.
     */
    CacheVersionedValue(CacheObject val, GridCacheVersion ver) {
        this.val = val;
        this.ver = ver;
    }

    /**
     * @return Cache version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Cache object.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * This method is called before the whole message is sent
     * and is responsible for pre-marshalling state.
     *
     * @param ctx Cache object context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(CacheObjectContext ctx) throws IgniteCheckedException {
        if (val != null)
            val.prepareMarshal(ctx);
    }

    /**
     * This method is called after the whole message is received
     * and is responsible for unmarshalling state.
     *
     * @param ctx Context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        if (val != null)
            val.finishUnmarshal(ctx.cacheObjectContext(), ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeMessage("val", val))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("ver", ver))
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

        switch (reader.state()) {
            case 0:
                val = reader.readMessage("val");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                ver = reader.readMessage("ver");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(CacheVersionedValue.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 102;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheVersionedValue.class, this);
    }
}
