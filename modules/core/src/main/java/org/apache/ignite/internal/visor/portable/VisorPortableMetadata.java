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

package org.apache.ignite.internal.visor.portable;

import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Portable object metadata to show in Visor.
 */
public class VisorPortableMetadata implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Type name */
    private String typeName;

    /** Type Id */
    private Integer typeId;

    /** Filed list */
    private Collection<VisorPortableMetadataField> fields;

    /** Type name */
    public String typeName() {
        return typeName;
    }

    public void typeName(String typeName) {
        this.typeName = typeName;
    }

    /** Type Id */
    public Integer typeId() {
        return typeId;
    }

    public void typeId(Integer typeId) {
        this.typeId = typeId;
    }

    /** Fields list */
    public Collection<VisorPortableMetadataField> fields() {
        return fields;
    }

    public void fields(Collection<VisorPortableMetadataField> fields) {
        this.fields = fields;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorPortableMetadata.class, this);
    }
}
