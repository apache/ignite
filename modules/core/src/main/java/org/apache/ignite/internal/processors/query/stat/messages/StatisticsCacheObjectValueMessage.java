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

package org.apache.ignite.internal.processors.query.stat.messages;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.cache.query.index.sorted.keys.CacheJavaObjectIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * H2 Cache object message.
 */
@IgniteCodeGeneratingFail
public class StatisticsCacheObjectValueMessage implements StatisticsValueMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 185;

    /** */
    private CacheObject obj;

    /**
     *
     */
    public StatisticsCacheObjectValueMessage() {
        // No-op.
    }

    /**
     * @param v Value.
     * @throws IgniteCheckedException If failed.
     */
    public StatisticsCacheObjectValueMessage(CacheJavaObjectIndexKey v) {
        obj = v.cacheObject();
    }

    /** {@inheritDoc} */
    @Override public IndexKey value(GridKernalContext ctx) throws IgniteCheckedException {
        return new CacheJavaObjectIndexKey(obj, ctx.query().objectContext());
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

        }

        return reader.afterMessageRead(StatisticsCacheObjectValueMessage.class);
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

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -22;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return String.valueOf(obj);
    }
}
