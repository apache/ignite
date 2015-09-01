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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class CacheEntrySerializablePredicate implements CacheEntryPredicate {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    private CacheEntryPredicate p;

    /** */
    private byte[] bytes;

    /**
     * Required by {@link org.apache.ignite.plugin.extensions.communication.Message}.
     */
    public CacheEntrySerializablePredicate() {
        // No-op.
    }

    /**
     * @param p Serializable predicate.
     */
    public CacheEntrySerializablePredicate(CacheEntryPredicate p) {
        assert p != null;

        this.p = p;
    }

    /**
     * @return Predicate.
     */
    public CacheEntryPredicate predicate() {
        return p;
    }

    /** {@inheritDoc} */
    @Override public void entryLocked(boolean locked) {
        assert p != null;

        p.entryLocked(locked);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert p != null || bytes != null;

        if (p == null) {
            p = ctx.marshaller().unmarshal(bytes, ldr);

            p.finishUnmarshal(ctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext ctx) throws IgniteCheckedException {
        assert p != null;

        p.prepareMarshal(ctx);

        bytes = ctx.marshaller().marshal(p);
    }

    /** {@inheritDoc} */
    @Override public boolean apply(GridCacheEntryEx e) {
        assert p != null;

        return p.apply(e);
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
                if (!writer.writeByteArray("bytes", bytes))
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
                bytes = reader.readByteArray("bytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(CacheEntrySerializablePredicate.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 99;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }
}