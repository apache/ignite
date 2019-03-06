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
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Entity
 */
public class WebSessionEntity implements Serializable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Session ID. */
    private String id;

    /** Creation time. */
    private long createTime;

    /** Last access time. */
    private long accessTime;

    /** Maximum inactive interval. */
    private int maxInactiveInterval;

    /** Attributes. */
    @GridToStringExclude
    private Map<String, byte[]> attrs;

    /**
     * Constructor.
     */
    public WebSessionEntity() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param id Session ID.
     * @param createTime Session create time.
     * @param accessTime Session last access time.
     * @param maxInactiveInterval Session will be removed if not accessed more then this value.
     */
    public WebSessionEntity(final String id, final long createTime, final long accessTime,
        final int maxInactiveInterval) {
        this.id = id;
        this.createTime = createTime;
        this.accessTime = accessTime;
        this.maxInactiveInterval = maxInactiveInterval;
    }

    /**
     * Constructor.
     */
    public WebSessionEntity(final WebSessionEntity other) {
        this(other.id(), other.createTime(), other.accessTime(), other.maxInactiveInterval());

        if (!other.attributes().isEmpty())
            attrs = new HashMap<>(other.attributes());
    }

    /**
     * @return Session ID.
     */
    public String id() {
        return id;
    }

    /**
     * @return Create time.
     */
    public long createTime() {
        return createTime;
    }

    /**
     * @return Access time.
     */
    public long accessTime() {
        return accessTime;
    }

    /**
     * Set access time.
     *
     * @param accessTime Access time.
     */
    public void accessTime(final long accessTime) {
        this.accessTime = accessTime;
    }

    /**
     * @return Max inactive interval.
     */
    public int maxInactiveInterval() {
        return maxInactiveInterval;
    }

    /**
     * Set max inactive interval;
     *
     * @param maxInactiveInterval Max inactive interval.
     */
    public void maxInactiveInterval(final int maxInactiveInterval) {
        this.maxInactiveInterval = maxInactiveInterval;
    }

    /**
     * @return Session attributes or {@link Collections#emptyMap()}.
     */
    public Map<String, byte[]> attributes() {
        return attrs == null ? Collections.<String, byte[]>emptyMap() : attrs;
    }

    /**
     * Add attribute to attribute map.
     *
     * @param name Attribute name.
     * @param val Attribute value.
     */
    public void putAttribute(final String name, final byte[] val) {
        if (attrs == null)
            attrs = new HashMap<>();

        attrs.put(name, val);
    }

    /**
     * Remove attribute.
     *
     * @param name Attribute name.
     */
    public void removeAttribute(final String name) {
        if (attrs != null)
            attrs.remove(name);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(final BinaryWriter writer) throws BinaryObjectException {
        final BinaryRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeString(id);
        rawWriter.writeLong(createTime);
        rawWriter.writeLong(accessTime);
        rawWriter.writeInt(maxInactiveInterval);
        rawWriter.writeMap(attrs);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(final BinaryReader reader) throws BinaryObjectException {
        final BinaryRawReader rawReader = reader.rawReader();

        id = rawReader.readString();
        createTime = rawReader.readLong();
        accessTime = rawReader.readLong();
        maxInactiveInterval = rawReader.readInt();
        attrs = rawReader.readMap();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebSessionEntity.class, this,
            "attributes", attrs != null ? attrs.keySet() : Collections.<String>emptySet());
    }
}
