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

package org.apache.ignite.internal.portable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Portable meta data implementation.
 */
public class BinaryMetaDataImpl implements BinaryType, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String typeName;

    /** */
    @GridToStringInclude
    private Map<String, String> fields;

    /** */
    private String affKeyFieldName;

    /**
     * For {@link Externalizable}.
     */
    public BinaryMetaDataImpl() {
        // No-op.
    }

    /**
     * @param typeName Type name.
     * @param fields Fields map.
     * @param affKeyFieldName Affinity key field name.
     */
    public BinaryMetaDataImpl(String typeName, @Nullable Map<String, String> fields,
        @Nullable String affKeyFieldName) {
        assert typeName != null;

        this.typeName = typeName;
        this.fields = fields;
        this.affKeyFieldName = affKeyFieldName;
    }

    /** {@inheritDoc} */
    @Override public String typeName() {
        return typeName;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> fields() {
        return fields != null ? fields.keySet() : Collections.<String>emptyList();
    }

    /**
     * @return Fields.
     */
    public Map<String, String> fields0() {
        return fields != null ? fields : Collections.<String, String>emptyMap();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fieldTypeName(String fieldName) {
        return fields != null ? fields.get(fieldName) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String affinityKeyFieldName() {
        return affKeyFieldName;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, typeName);
        U.writeMap(out, fields);
        U.writeString(out, affKeyFieldName);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        typeName = U.readString(in);
        fields = U.readMap(in);
        affKeyFieldName = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryMetaDataImpl.class, this);
    }
}