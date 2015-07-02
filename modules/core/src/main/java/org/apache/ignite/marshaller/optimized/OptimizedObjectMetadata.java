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

import java.io.*;
import java.util.*;

/**
 * Metadata that keeps fields' name to id mapping.
 */
public class OptimizedObjectMetadata implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private LinkedHashMap<Integer, FieldInfo> indexes;

    /** */
    private short counter;

    /** Constructor. */
    public OptimizedObjectMetadata() {
        // No-op
    }

    /**
     * Adds field to metadata list assigning an index to it that is used to locate field bucket in the footer of
     * object's serialized form.
     *
     * @param fieldName Field name.
     */
    public void addField(String fieldName, OptimizedFieldType type) {
        if (indexes == null)
            indexes = new LinkedHashMap<>();

        indexes.put(OptimizedMarshallerUtils.resolveFieldId(fieldName), new FieldInfo(type, counter++));
    }

    /**
     * Returns an index assigned to the field. The index is used to locate field bucket in the footer of
     * object's serialized form.
     *
     * @param fieldName Field name.
     * @return Index.
     * @throws IgniteFieldNotFoundException If object doesn't have such a field.
     */
    public int fieldIndex(String fieldName) throws IgniteFieldNotFoundException {
        if (indexes == null)
            throw new IgniteFieldNotFoundException("Object doesn't have field named: " + fieldName);

        FieldInfo info = indexes.get(OptimizedMarshallerUtils.resolveFieldId(fieldName));

        if (info == null)
            throw new IgniteFieldNotFoundException("Object doesn't have field named: " + fieldName);

        return info.index();
    }

    /**
     * Returns meta list.
     *
     * @return Meta list.
     */
    public Set<Map.Entry<Integer, FieldInfo>> metaList() {
        return indexes.entrySet();
    }

    /**
     * Returns number of fields.
     *
     * @return Number of fields.
     */
    public int size() {
        return indexes.size();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(indexes);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        indexes = (LinkedHashMap<Integer, FieldInfo>)in.readObject();
    }

    /**
     * Field info.
     */
    public static class FieldInfo implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private OptimizedFieldType type;

        /** */
        private short index;

        /**
         * Constructor.
         * @param type
         * @param index
         */
        public FieldInfo(OptimizedFieldType type, short index) {
            this.type = type;
            this.index = index;
        }

        public OptimizedFieldType type() {
            return type;
        }

        public short index() {
            return index;
        }
    }
}
