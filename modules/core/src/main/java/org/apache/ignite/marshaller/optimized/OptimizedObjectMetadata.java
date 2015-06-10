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

package org.apache.ignite.marshaller.optimized;

import org.apache.ignite.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.marshaller.optimized.OptimizedFieldType.*;
/**
 * Metadata that keeps fields information. Used in conjunction with the footer that is added to some objects during
 * marshalling.
 */
public class OptimizedObjectMetadata implements Externalizable {
    /** */
    private List<FieldInfo> fieldsInfo;

    /** Constructor. */
    public OptimizedObjectMetadata() {
        // No-op
    }

    /**
     * Adds meta for a new field.
     *
     * @param fieldId Field ID.
     * @param fieldType Field type.
     */
    public void addMeta(int fieldId, OptimizedFieldType fieldType) {
        if (fieldsInfo == null)
            fieldsInfo = new ArrayList<>();

        int len = 1;

        switch (fieldType) {
            case BYTE:
            case BOOLEAN:
                len += 1;
                break;

            case SHORT:
            case CHAR:
                len += 2;
                break;

            case INT:
            case FLOAT:
                len += 4;
                break;

            case LONG:
            case DOUBLE:
                len += 8;
                break;

            case OTHER:
                len = OptimizedMarshallerUtils.VARIABLE_LEN;
                break;

            default:
                throw new IgniteException("Unknown field type: " + fieldType);
        }

        assert len != 1;

        fieldsInfo.add(new FieldInfo(fieldId, len));
    }

    /**
     * Gets {@link org.apache.ignite.marshaller.optimized.OptimizedObjectMetadata.FieldInfo} at the {@code index}.
     *
     * @param index Position.
     * @return Field meta info.
     */
    public FieldInfo getMeta(int index) {
        return fieldsInfo.get(index);
    }
    /**
     * Returns all the metadata stored for the object.
     *
     * @return Metadata collection.
     */
    public List<FieldInfo> getMeta() {
        return Collections.unmodifiableList(fieldsInfo);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        if (fieldsInfo == null) {
            out.writeInt(0);
            return;
        }

        out.writeInt(fieldsInfo.size());

        for (FieldInfo fieldInfo : fieldsInfo) {
            out.writeInt(fieldInfo.id);
            out.writeInt(fieldInfo.len);
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        fieldsInfo = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            fieldsInfo.add(new FieldInfo(in.readInt(), in.readInt()));
    }

    /**
     * Field info.
     */
    public static class FieldInfo {
        /** Field ID. */
        int id;

        /** Field type. */
        int len;

        /**
         * Constructor.
         *
         * @param id Field ID.
         * @param len Field len.
         */
        public FieldInfo(int id, int len) {
            this.id = id;
            this.len = len;
        }
    }
}
