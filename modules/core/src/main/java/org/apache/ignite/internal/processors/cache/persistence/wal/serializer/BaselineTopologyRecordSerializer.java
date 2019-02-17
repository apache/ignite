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

package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.BaselineTopologyRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * {@link BaselineTopologyRecord} WAL serializer.
 */
public class BaselineTopologyRecordSerializer {
    /** Cache shared context. */
    private GridCacheSharedContext cctx;

    /** Class loader to unmarshal consistent IDs. */
    private ClassLoader clsLdr;

    /**
     * Create an instance of serializer.
     *
     * @param cctx Cache shared context.
     */
    public BaselineTopologyRecordSerializer(GridCacheSharedContext cctx) {
        this.cctx = cctx;

        clsLdr = U.resolveClassLoader(cctx.gridConfig());
    }

    /**
     * Writes {@link BaselineTopologyRecord} to given buffer.
     *
     * @param rec {@link BaselineTopologyRecord} instance.
     * @param buf Byte buffer.
     * @throws IgniteCheckedException In case of fail.
     */
    public void write(BaselineTopologyRecord rec, ByteBuffer buf) throws IgniteCheckedException {
        buf.putInt(rec.id());

        Map<Short, Object> mapping = rec.mapping();

        if (mapping != null && !mapping.isEmpty()) {
            buf.putInt(mapping.size());

            for (Map.Entry<Short, Object> e : mapping.entrySet()) {
                buf.putShort(e.getKey());

                writeConsistentId(e.getValue(), buf);
            }
        }
        else
            buf.putInt(0);
    }

    /**
     * Reads {@link BaselineTopologyRecord} from given input.
     *
     * @param in Input
     * @return BaselineTopologyRecord instance.
     * @throws IOException In case of fail.
     * @throws IgniteCheckedException In case of fail.
     */
    public BaselineTopologyRecord read(ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException {
        int id = in.readInt();

        int size = in.readInt();

        Map<Short, Object> mapping = size > 0 ? U.<Short, Object>newHashMap(size) : null;

        for (int i = 0; i < size; i++) {
            short compactId = in.readShort();

            Object consistentId = readConsistentId(in);

            mapping.put(compactId, consistentId);
        }

        return new BaselineTopologyRecord(id, mapping);
    }

    /**
     * Returns size of marshalled {@link BaselineTopologyRecord} in bytes.
     *
     * @param rec BaselineTopologyRecord instance.
     * @return Size of BaselineTopologyRecord instance in bytes.
     * @throws IgniteCheckedException In case of fail.
     */
    public int size(BaselineTopologyRecord rec) throws IgniteCheckedException {
        int size = 0;

        size += /* Baseline topology ID. */ 4;

        size += /* Consistent ID mapping size. */ 4;

        if (rec.mapping() != null) {
            for (Object consistentId : rec.mapping().values()) {
                size += /* Compact ID size */ 2;

                size += marshalConsistentId(consistentId).length;
            }
        }

        return size;
    }

    /**
     * Write consistent id to given buffer.
     *
     * @param consistentId Consistent id.
     * @param buf Byte buffer.
     * @throws IgniteCheckedException In case of fail.
     */
    private void writeConsistentId(Object consistentId, ByteBuffer buf) throws IgniteCheckedException {
        byte[] content = marshalConsistentId(consistentId);

        buf.putInt(content.length);
        buf.put(content);
    }

    /**
     * Read consistent id from given input.
     *
     * @param in Input.
     * @return Consistent id.
     * @throws IOException In case of fail.
     * @throws IgniteCheckedException In case of fail.
     */
    private Object readConsistentId(ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException {
        int len = in.readInt();
        in.ensure(len);

        byte[] content = new byte[len];
        in.readFully(content);

        return cctx.marshaller().unmarshal(content, clsLdr);
    }

    /**
     * Marshal consistent id to byte array.
     *
     * @param consistentId Consistent id.
     * @return Marshalled byte array.
     * @throws IgniteCheckedException In case of fail.
     */
    private byte[] marshalConsistentId(Object consistentId) throws IgniteCheckedException {
        return cctx.marshaller().marshal(consistentId);
    }
}
