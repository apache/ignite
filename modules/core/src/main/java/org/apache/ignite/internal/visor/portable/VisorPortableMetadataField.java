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

/**
 * Portable object metadata field information.
 */
public class VisorPortableMetadataField implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Field name. */
    private String fieldName;

    /** Field type name. */
    private String fieldTypeName;

    /** Field id. */
    private Integer fieldId;

    /** Field name. */
    public String fieldName() {
        return fieldName;
    }

    public void fieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    /** Field type name. */
    public String fieldTypeName() {
        return fieldTypeName;
    }

    public void fieldTypeName(String fieldTypeName) {
        this.fieldTypeName = fieldTypeName;
    }

    /** Field id. */
    public Integer fieldId() {
        return fieldId;
    }

    public void fieldId(Integer fieldId) {
        this.fieldId = fieldId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorPortableMetadataField.class, this);
    }
}
