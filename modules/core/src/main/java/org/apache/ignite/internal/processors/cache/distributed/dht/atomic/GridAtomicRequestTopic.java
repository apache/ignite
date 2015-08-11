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

import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 */
class GridAtomicRequestTopic implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final byte NEAR_UPDATE_REQ = 1;

    /** */
    private static final byte DHT_UPDATE_REQ = 2;

    /** */
    private int cacheId;

    /** */
    private int part;

    /** */
    private byte type;

    /**
     * Near request topic.
     *
     * @param cacheId Cache ID.
     * @param part Partition.
     * @return Topic.
     */
    static GridAtomicRequestTopic nearUpdateRequest(int cacheId, int part) {
        return new GridAtomicRequestTopic(cacheId, part, NEAR_UPDATE_REQ);
    }

    /**
     * DHT request topic.
     *
     * @param cacheId Cache ID.
     * @param part Partition.
     * @return Topic.
     */
    static GridAtomicRequestTopic dhtUpdateRequest(int cacheId, int part) {
        return new GridAtomicRequestTopic(cacheId, part, DHT_UPDATE_REQ);
    }

    /**
     * For {@link Externalizable}.
     */
    public GridAtomicRequestTopic() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param part Partition.
     * @param type Type.
     */
    private GridAtomicRequestTopic(int cacheId, int part, byte type) {
        this.cacheId = cacheId;
        this.part = part;
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GridAtomicRequestTopic topic = (GridAtomicRequestTopic)o;

        return cacheId == topic.cacheId && part == topic.part && type == topic.type;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = cacheId;

        res = 31 * res + part;
        res = 31 * res + type;

        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(cacheId);
        out.writeInt(part);
        out.writeByte(type);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cacheId = in.readInt();
        part = in.readInt();
        type = in.readByte();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridAtomicRequestTopic.class, this);
    }
}
