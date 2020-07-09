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

package org.apache.ignite.internal.websession;

import java.io.Serializable;
import java.util.Map;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Updates web session attributes according to {@link #updatesMap} and {@link #accessTime},
 * {@link #maxInactiveInterval}.
 */
public class WebSessionAttributeProcessor implements EntryProcessor<String, WebSessionEntity, Void>,
    Serializable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Attribute updates. */
    private Map<String, byte[]> updatesMap;

    /** Access time update. */
    private long accessTime;

    /** Max inactive interval update. */
    private int maxInactiveInterval;

    /** Indicates whether apply or not max inactive interval update. */
    private boolean maxIntervalChanged;

    /**
     * Empty constructor for serialization.
     */
    public WebSessionAttributeProcessor() {
        // No-op.
    }

    /**
     * Constructs attribute processor.
     *
     * @param updatesMap Updates that should be performed on entity attributes.
     * @param accessTime Access time.
     * @param maxInactiveInterval Max inactive interval.
     * @param maxIntervalChanged {@code True} if max inactive interval should be updated.
     */
    public WebSessionAttributeProcessor(final Map<String, byte[]> updatesMap, final long accessTime,
        final int maxInactiveInterval, final boolean maxIntervalChanged) {
        this.updatesMap = updatesMap;
        this.accessTime = accessTime;
        this.maxInactiveInterval = maxInactiveInterval;
        this.maxIntervalChanged = maxIntervalChanged;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(final BinaryWriter writer) throws BinaryObjectException {
        final BinaryRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeMap(updatesMap);
        rawWriter.writeLong(accessTime);
        rawWriter.writeBoolean(maxIntervalChanged);

        if (maxIntervalChanged)
            rawWriter.writeInt(maxInactiveInterval);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(final BinaryReader reader) throws BinaryObjectException {
        final BinaryRawReader rawReader = reader.rawReader();

        updatesMap = rawReader.readMap();
        accessTime = rawReader.readLong();
        maxIntervalChanged = rawReader.readBoolean();

        if (maxIntervalChanged)
            maxInactiveInterval = rawReader.readInt();
    }

    /** {@inheritDoc} */
    @Override public Void process(final MutableEntry<String, WebSessionEntity> entry,
        final Object... arguments) throws EntryProcessorException {
        final WebSessionEntity entity = entry.getValue();

        final WebSessionEntity newEntity = new WebSessionEntity(entity);

        if (newEntity.accessTime() < accessTime)
            newEntity.accessTime(accessTime);

        if (maxIntervalChanged)
            newEntity.maxInactiveInterval(maxInactiveInterval);

        if (!F.isEmpty(updatesMap)) {
            for (final Map.Entry<String, byte[]> update : updatesMap.entrySet()) {
                final String name = update.getKey();

                assert name != null;

                final byte[] val = update.getValue();

                if (val != null)
                    newEntity.putAttribute(name, val);
                else
                    newEntity.removeAttribute(name);
            }
        }

        entry.setValue(newEntity);

        return null;
    }
}
