/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.websession;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.util.typedef.F;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.Map;

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
