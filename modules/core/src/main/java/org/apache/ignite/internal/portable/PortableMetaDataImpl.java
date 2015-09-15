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
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableMarshalAware;
import org.apache.ignite.portable.PortableMetadata;
import org.apache.ignite.portable.PortableRawReader;
import org.apache.ignite.portable.PortableRawWriter;
import org.apache.ignite.portable.PortableReader;
import org.apache.ignite.portable.PortableWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Portable meta data implementation.
 */
public class PortableMetaDataImpl implements PortableMetadata, PortableMarshalAware, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String typeName;

    /** */
    @GridToStringInclude
    private Map<String, String> fields;

    /** */
    private volatile Map<Integer, String> fldIdToName;

    /** */
    private String affKeyFieldName;

    /**
     * For {@link Externalizable}.
     */
    public PortableMetaDataImpl() {
        // No-op.
    }

    /**
     * @param typeName Type name.
     * @param fields Fields map.
     * @param affKeyFieldName Affinity key field name.
     */
    public PortableMetaDataImpl(String typeName, @Nullable Map<String, String> fields,
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

    /**
     * @return Fields meta data.
     */
    public Map<String, String> fieldsMeta() {
        return fields != null ? fields : Collections.<String, String>emptyMap();
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
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        PortableRawWriter raw = writer.rawWriter();

        raw.writeString(typeName);
        raw.writeString(affKeyFieldName);
        raw.writeMap(fields);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        PortableRawReader raw = reader.rawReader();

        typeName = raw.readString();
        affKeyFieldName = raw.readString();
        fields = raw.readMap();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PortableMetaDataImpl.class, this);
    }
}