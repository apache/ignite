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

package org.gridgain.grid.kernal.processors.rest.client.message;

import org.apache.ignite.portables.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

/**
 * Portable meta data sent from client.
 */
public class GridClientPortableMetaData implements PortableMarshalAware {
    /** */
    private int typeId;

    /** */
    private String typeName;

    /** */
    private Map<String, Integer> fields;

    /** */
    private String affKeyFieldName;

    /**
     * @return Type ID.
     */
    public int typeId() {
        return typeId;
    }

    /**
     * @return Type name.
     */
    public String typeName() {
        return typeName;
    }

    /**
     * @return Fields.
     */
    public Map<String, Integer> fields() {
        return fields;
    }

    /**
     * @return Affinity key field name.
     */
    public String affinityKeyFieldName() {
        return affKeyFieldName;
    }

    /** {@inheritDoc} */
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        PortableRawWriter raw = writer.rawWriter();

        raw.writeInt(typeId);
        raw.writeString(typeName);
        raw.writeString(affKeyFieldName);
        raw.writeMap(fields);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        PortableRawReader raw = reader.rawReader();

        typeId = raw.readInt();
        typeName = raw.readString();
        affKeyFieldName = raw.readString();
        fields = raw.readMap();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientPortableMetaData.class, this);
    }
}
