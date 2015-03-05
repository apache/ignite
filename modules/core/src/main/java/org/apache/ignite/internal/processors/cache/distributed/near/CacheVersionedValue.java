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
import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;

/**
 * Cache object and version.
 */
public class CacheVersionedValue implements Message {
    /** Cache version. */
    private GridCacheVersion vers;

    /** Cache object. */
    private CacheObject obj;

    /** */
    public CacheVersionedValue() {
        // No-op.
    }

    /**
     * @param vers Cache version.
     * @param obj Cache object.
     */
    CacheVersionedValue(GridCacheVersion vers, CacheObject obj) {
        this.vers = vers;
        this.obj = obj;
    }

    /**
     * @return Cache version.
     */
    public GridCacheVersion version() {
        return vers;
    }

    /**
     * @return Cache object.
     */
    public CacheObject cacheObject() {
        return obj;
    }

    /**
     * This method is called before the whole message is sent
     * and is responsible for pre-marshalling state.
     *
     * @param ctx Cache object context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(CacheObjectContext ctx) throws IgniteCheckedException {
        if (obj != null)
            obj.prepareMarshal(ctx);
    }

    /**
     * This method is called after the whole message is recived
     * and is responsible for unmarshalling state.
     *
     * @param ctx Context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        if (obj != null)
            obj.finishUnmarshal(ctx.cacheObjectContext(), ldr);
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
                if (!writer.writeMessage("obj", obj))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("vers", vers))
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
                obj = reader.readMessage("obj");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                vers = reader.readMessage("vers");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 102;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }
}
